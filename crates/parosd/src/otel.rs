//! OpenTelemetry export (#366 (metrics)): `parosd` pushes its spans and its
//! metrics to a collector over OTLP/gRPC (`PAROS_OTLP_*`).
//!
//! The metrics are recorded once, in the machine's `prometheus` registry
//! ([`paros::metrics::Metrics`]); `/metrics` renders it and this module
//! reads it. Each paros family becomes one observable instrument (a
//! counter for a `_total`, a gauge otherwise) whose callback reads that
//! family's series from the registry at every push, labels included. The
//! OpenTelemetry name drops the `_total` suffix (`paros_calls`), as the
//! OpenTelemetry naming rules ask; a collector's Prometheus exporter adds
//! it back.
//!
//! The spans are the ones `tracing` already records (`RUST_LOG` filters
//! them), through `tracing-opentelemetry`.

use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{AsyncInstrument, MeterProvider as _};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use paros::metrics::{FamilyKind, Metrics};

use crate::settings::Settings;

/// The exporters `parosd` runs, shut down (and so flushed) at its exit.
#[derive(Default)]
pub struct Telemetry {
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
}

impl Telemetry {
    /// Start the exporters `settings` asks for, with `metrics` as the
    /// metrics source. Must run inside the Tokio runtime: the gRPC
    /// channels live on it.
    pub fn start(settings: &Settings, metrics: &Metrics) -> Result<Self, String> {
        let resource = Resource::builder()
            .with_service_name("parosd")
            .with_attribute(KeyValue::new(
                "service.instance.id",
                settings.instance().to_string(),
            ))
            .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
            .build();
        let mut telemetry = Self::default();
        if let Some(endpoint) = settings.traces_endpoint() {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .map_err(|error| format!("PAROS_OTLP traces {endpoint}: {error}"))?;
            telemetry.tracer = Some(
                SdkTracerProvider::builder()
                    .with_batch_exporter(exporter)
                    .with_resource(resource.clone())
                    .build(),
            );
        }
        if let Some(endpoint) = settings.metrics_endpoint() {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .map_err(|error| format!("PAROS_OTLP metrics {endpoint}: {error}"))?;
            let reader = PeriodicReader::builder(exporter)
                .with_interval(Duration::from_millis(settings.otlp_interval_ms))
                .build();
            let meter = SdkMeterProvider::builder()
                .with_reader(reader)
                .with_resource(resource)
                .build();
            observe(&meter, metrics);
            telemetry.meter = Some(meter);
        }
        Ok(telemetry)
    }

    /// The tracer the `tracing` layer hands spans to, if spans are pushed.
    pub fn tracer(&self) -> Option<SdkTracer> {
        self.tracer
            .as_ref()
            .map(|provider| provider.tracer("parosd"))
    }

    /// Flush and stop every exporter.
    pub fn shutdown(self) {
        if let Some(tracer) = self.tracer
            && let Err(error) = tracer.shutdown()
        {
            eprintln!("parosd: OTLP traces: {error}");
        }
        if let Some(meter) = self.meter
            && let Err(error) = meter.shutdown()
        {
            eprintln!("parosd: OTLP metrics: {error}");
        }
    }
}

/// One observable instrument per paros family, each read from `metrics`'
/// registry at every push.
fn observe(meter: &SdkMeterProvider, metrics: &Metrics) {
    let meter = meter.meter("paros");
    for family in metrics.families() {
        let name = otel_name(&family.name);
        let read = reader(metrics.clone(), family.name);
        match family.kind {
            FamilyKind::Counter => {
                meter
                    .f64_observable_counter(name)
                    .with_description(family.help)
                    .with_callback(read)
                    .build();
            }
            FamilyKind::Gauge => {
                meter
                    .f64_observable_gauge(name)
                    .with_description(family.help)
                    .with_callback(read)
                    .build();
            }
        }
    }
}

/// The callback that observes every series of family `name`.
fn reader(metrics: Metrics, name: String) -> impl Fn(&dyn AsyncInstrument<f64>) + Send + Sync {
    move |instrument| {
        let Some(registry) = metrics.registry() else {
            return;
        };
        for family in registry.gather() {
            if family.name() != name {
                continue;
            }
            for series in family.get_metric() {
                let value = match family.get_field_type() {
                    prometheus::proto::MetricType::COUNTER => series.get_counter().get_value(),
                    _ => series.get_gauge().get_value(),
                };
                let labels: Vec<KeyValue> = series
                    .get_label()
                    .iter()
                    .map(|pair| KeyValue::new(pair.name().to_string(), pair.value().to_string()))
                    .collect();
                instrument.observe(value, &labels);
            }
        }
    }
}

/// The crates whose spans carry the export itself: never exported, or
/// each push would trace the push.
const UNEXPORTED: [&str; 6] = ["h2", "hyper", "tonic", "tower", "opentelemetry", "reqwest"];

/// Whether a span or event goes to the collector: not the export's own.
pub fn exported(metadata: &tracing::Metadata<'_>) -> bool {
    let target = metadata.target();
    !UNEXPORTED.iter().any(|unexported| {
        target
            .strip_prefix(unexported)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with("::") || rest.starts_with('_'))
    })
}

/// The OpenTelemetry name of a Prometheus family: without `_total`.
fn otel_name(prometheus: &str) -> String {
    prometheus
        .strip_suffix("_total")
        .unwrap_or(prometheus)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::{exported, otel_name};

    #[test]
    fn a_counter_drops_its_total_suffix() {
        assert_eq!(otel_name("paros_calls_total"), "paros_calls");
        assert_eq!(otel_name("paros_applied_slot"), "paros_applied_slot");
    }

    #[test]
    fn the_exports_own_spans_stay_home() {
        let unexported = [
            "h2::codec",
            "tonic",
            "opentelemetry_sdk::metrics",
            "hyper_util",
        ];
        let kept = ["paros::driver", "parosd", "moonpool_rpc", "towers"];
        let target = |name: &'static str| {
            tracing::Metadata::new(
                "span",
                name,
                tracing::Level::INFO,
                None,
                None,
                None,
                tracing::field::FieldSet::new(&[], tracing::callsite::Identifier(&CALLSITE)),
                tracing::metadata::Kind::SPAN,
            )
        };
        for name in unexported {
            assert!(!exported(&target(name)), "{name}");
        }
        for name in kept {
            assert!(exported(&target(name)), "{name}");
        }
    }

    struct Callsite;
    static CALLSITE: Callsite = Callsite;
    impl tracing::callsite::Callsite for Callsite {
        fn set_interest(&self, _: tracing::subscriber::Interest) {}
        fn metadata(&self) -> &tracing::Metadata<'_> {
            unreachable!("never registered")
        }
    }
}
