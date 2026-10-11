use std::time::Duration;

use moonpool_core::{DiskCounters, SystemSample};

use super::Busyness;

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn sample(at: u64, cpu: u64, io_ticks: u64, reads: u64) -> SystemSample {
    SystemSample {
        at: ms(at),
        process_cpu: ms(cpu),
        cores: 8,
        machine_busy: ms(cpu * 2),
        machine_total: ms(at * 8),
        run_loop_busy: Some(ms(cpu)),
        disk: Some(DiskCounters {
            device: "nvme0n1".to_string(),
            io_ticks: ms(io_ticks),
            in_flight: 3,
            reads,
            writes: reads * 2,
            read_bytes: reads * 4096,
            write_bytes: reads * 8192,
            read_time: ms(reads),
            write_time: ms(reads),
        }),
    }
}

#[test]
fn a_window_is_the_ratio_of_the_deltas() {
    let busy =
        Busyness::between(&sample(0, 0, 0, 0), &sample(5000, 4400, 2500, 1000)).expect("a window");
    assert_eq!(busy.elapsed, ms(5000));
    assert!((busy.cpu_cores - 0.88).abs() < 1e-12);
    assert_eq!(busy.cores, 8);
    assert!((busy.machine_cpu - 8800.0 / 40_000.0).abs() < 1e-12);
    assert!((busy.run_loop_busy.expect("run loop") - 0.88).abs() < 1e-12);
    assert!(busy.in_range());
    let disk = busy.disk.expect("disk");
    assert_eq!(disk.device, "nvme0n1");
    assert!((disk.busy - 0.5).abs() < 1e-12);
    assert_eq!(disk.queue_depth, 3);
    assert!((disk.reads_hz - 200.0).abs() < 1e-9);
    assert!((disk.writes_hz - 400.0).abs() < 1e-9);
    assert!((disk.read_bps - 200.0 * 4096.0).abs() < 1e-6);
}

#[test]
fn ratios_are_clamped_and_cpu_cores_is_not() {
    // Tokio adds a parked worker's busy time at once: a jump above the
    // window, clamped. A process on many cores uses more than one.
    let mut now = sample(1000, 3000, 5000, 0);
    now.run_loop_busy = Some(ms(4000));
    let busy = Busyness::between(&sample(0, 0, 0, 0), &now).expect("a window");
    assert!((busy.cpu_cores - 3.0).abs() < 1e-12);
    assert!((busy.run_loop_busy.expect("run loop") - 1.0).abs() < f64::EPSILON);
    assert!((busy.disk.expect("disk").busy - 1.0).abs() < f64::EPSILON);
}

#[test]
fn a_counter_that_goes_back_gives_no_window() {
    let prev = sample(1000, 500, 500, 10);
    assert_eq!(Busyness::between(&prev, &sample(1000, 600, 600, 11)), None);
    assert_eq!(Busyness::between(&prev, &sample(900, 600, 600, 11)), None);
    assert_eq!(Busyness::between(&prev, &sample(2000, 400, 600, 11)), None);
    assert_eq!(Busyness::between(&prev, &sample(2000, 600, 400, 11)), None);
    assert_eq!(Busyness::between(&prev, &sample(2000, 600, 600, 9)), None);
    let mut rebooted = sample(2000, 600, 600, 11);
    rebooted.run_loop_busy = Some(Duration::ZERO);
    assert_eq!(Busyness::between(&prev, &rebooted), None);
}

#[test]
fn an_unknown_or_changed_device_gives_no_disk() {
    let prev = sample(0, 0, 0, 0);
    let mut now = sample(1000, 100, 100, 1);
    now.disk = None;
    assert_eq!(Busyness::between(&prev, &now).expect("a window").disk, None);
    let mut moved = sample(1000, 100, 100, 1);
    if let Some(disk) = moved.disk.as_mut() {
        disk.device = "sdb".to_string();
    }
    assert_eq!(
        Busyness::between(&prev, &moved).expect("a window").disk,
        None
    );
    let mut no_run_loop = sample(1000, 100, 100, 1);
    no_run_loop.run_loop_busy = None;
    assert_eq!(
        Busyness::between(&prev, &no_run_loop)
            .expect("a window")
            .run_loop_busy,
        None
    );
}
