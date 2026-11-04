//! OpenTelemetry export (#366 (metrics)): `parosd` pushes its spans and its
//! metrics to a collector over OTLP/gRPC (`PAROS_OTLP_*`).
//!
//! The metrics are recorded once, in the machine's `prometheus` registry
//! ([`paros::observe::metrics::Metrics`]); `/metrics` renders it and this module
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
use paros::observe::metrics::{FamilyKind, Metrics};

use crate::settings::Settings;

/// The exporters `parosd` runs, shut down (and so flushed) at its exit.
#[derive(Default)]
pub struct Telemetry {
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
}

impl Telemetry {
    /// Start the exporters `settings` asks for, with `metrics` as the
    /// metrics source, and the runtime they run on.
    ///
    /// The machine runs on compio, but tonic's gRPC channels need a Tokio
    /// runtime. When `settings` asks for an exporter, this starts one with
    /// a single worker thread (`parosd-otlp`) and builds the exporters in
    /// it. Keep the runtime until [`Telemetry::shutdown`] returns: the
    /// exporters flush over it. Without an exporter, no runtime starts.
    pub fn start(
        settings: &Settings,
        metrics: &Metrics,
    ) -> Result<(Self, Option<tokio::runtime::Runtime>), String> {
        if settings.traces_endpoint().is_none() && settings.metrics_endpoint().is_none() {
            return Ok((Self::default(), None));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("parosd-otlp")
            .enable_all()
            .build()
            .map_err(|error| format!("PAROS_OTLP: cannot start the exporters' runtime: {error}"))?;
        let telemetry = {
            let _entered = runtime.enter();
            Self::exporters(settings, metrics)?
        };
        Ok((telemetry, Some(runtime)))
    }

    /// Build the exporters; inside the exporters' Tokio runtime.
    fn exporters(settings: &Settings, metrics: &Metrics) -> Result<Self, String> {
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
    use std::sync::{Arc, Weak};
    use std::time::Duration;

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader;
    use opentelemetry_sdk::metrics::{
        InstrumentKind, ManualReader, Pipeline, SdkMeterProvider, Temporality,
    };
    use paros::core::journal::{JournalId, JournalIdentifier, TenantId};
    use paros::observe::metrics::{CallKind, Metrics, Phase};

    use clap::Parser as _;

    use super::{Telemetry, exported, observe, otel_name};
    use crate::settings::Settings;

    /// A manual reader the test keeps a handle on after the provider takes it.
    #[derive(Clone, Debug)]
    struct Shared(Arc<ManualReader>);

    impl MetricReader for Shared {
        fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
            self.0.register_pipeline(pipeline);
        }
        fn collect(&self, metrics: &mut ResourceMetrics) -> OTelSdkResult {
            self.0.collect(metrics)
        }
        fn force_flush(&self) -> OTelSdkResult {
            self.0.force_flush()
        }
        fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
            self.0.shutdown_with_timeout(timeout)
        }
        fn temporality(&self, kind: InstrumentKind) -> Temporality {
            self.0.temporality(kind)
        }
    }

    /// One data point: its instrument's name, its value and its labels.
    type Point = (String, f64, Vec<(String, String)>);

    /// Every data point the reader collects.
    fn collect(reader: &Shared) -> Vec<Point> {
        let mut gathered = ResourceMetrics::default();
        reader.collect(&mut gathered).expect("collected");
        let mut points = Vec::new();
        for scope in gathered.scope_metrics() {
            for metric in scope.metrics() {
                let labels = |attributes: &mut dyn Iterator<Item = &opentelemetry::KeyValue>| {
                    attributes
                        .map(|pair| (pair.key.to_string(), pair.value.to_string()))
                        .collect::<Vec<_>>()
                };
                match metric.data() {
                    AggregatedMetrics::F64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            let labels = labels(&mut point.attributes());
                            points.push((metric.name().to_string(), point.value(), labels));
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Gauge(gauge)) => {
                        for point in gauge.data_points() {
                            let labels = labels(&mut point.attributes());
                            points.push((metric.name().to_string(), point.value(), labels));
                        }
                    }
                    other => panic!("an unexpected aggregation: {other:?}"),
                }
            }
        }
        points
    }

    #[test]
    fn every_series_of_the_registry_is_observed_with_its_labels() {
        let metrics = Metrics::on();
        metrics.set_phase(Phase::Serving);
        let id = JournalIdentifier {
            tenant: TenantId(1),
            journal: JournalId(2),
        };
        let journal = metrics
            .journal(id, || ("acme".into(), "orders".into()))
            .expect("metrics on");
        journal.received(CallKind::Write);
        journal.received(CallKind::Write);
        metrics.settled(id, 4, 5, Duration::from_millis(1));
        let reader = Shared(Arc::new(ManualReader::builder().build()));
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        observe(&provider, &metrics);
        let points = collect(&reader);
        let value = |name: &str| {
            points
                .iter()
                .filter(|(found, ..)| found == name)
                .map(|(_, value, _)| *value)
                .sum::<f64>()
        };
        assert_eq!(value("paros_settles"), 1.0, "a counter drops _total");
        assert_eq!(value("paros_machine_phase"), 2.0, "serving");
        assert_eq!(value("paros_applied_slot"), 4.0);
        assert_eq!(value("paros_chosen_slot"), 5.0);
        let writes = points
            .iter()
            .find(|(name, _, labels)| {
                name == "paros_calls" && labels.contains(&("call".into(), "write".into()))
            })
            .expect("the write series");
        assert_eq!(writes.1, 2.0);
        assert!(
            writes.2.contains(&("tenant".into(), "acme".into())),
            "{writes:?}"
        );
        assert!(points.iter().all(|(name, ..)| !name.ends_with("_total")));
        // An instrument per family, and only the registry's.
        let families: std::collections::BTreeSet<_> = metrics
            .families()
            .into_iter()
            .map(|f| otel_name(&f.name))
            .collect();
        assert!(points.iter().all(|(name, ..)| families.contains(name)));
    }

    #[test]
    fn metrics_off_observe_nothing() {
        let reader = Shared(Arc::new(ManualReader::builder().build()));
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        observe(&provider, &Metrics::off());
        assert!(collect(&reader).is_empty());
        provider.meter("unused");
    }

    #[test]
    fn no_endpoint_starts_no_exporter_and_no_runtime() {
        let settings =
            Settings::try_parse_from(["parosd", "--listen", "127.0.0.1:1", "--data-dir", "/x"])
                .expect("valid");
        let (telemetry, runtime) = Telemetry::start(&settings, &Metrics::on()).expect("started");
        assert!(runtime.is_none());
        assert!(telemetry.tracer().is_none());
        telemetry.shutdown();
    }

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
        let kept = ["paros::roles::node", "parosd", "moonpool_rpc", "towers"];
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
