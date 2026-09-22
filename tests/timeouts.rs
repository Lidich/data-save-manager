use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::process::Command;
use std::time::Duration;

use data_save_manager::WriteTimeouts;

const KEYS: [&str; 3] = [
    "DATA_SAVE_BATCH_TIMEOUT_MS",
    "DATA_SAVE_STATEMENT_TIMEOUT_MS",
    "DATA_SAVE_LOCK_TIMEOUT_MS",
];

fn probe(overrides: &[(&str, OsString)], expected: &str) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", "env_probe", "--nocapture"]);
    for key in KEYS {
        command.env_remove(key);
    }
    for (key, value) in overrides {
        command.env(key, value);
    }
    let output = command
        .env("TIMEOUT_TEST_EXPECTED", expected)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn env_probe() {
    let Ok(expected) = std::env::var("TIMEOUT_TEST_EXPECTED") else {
        return;
    };
    let result = WriteTimeouts::from_env();
    if let Some(key) = expected.strip_prefix("error:") {
        let error = result.unwrap_err().to_string();
        assert!(error.contains(key), "{error}");
        assert!(!error.contains("secret-invalid-value"), "{error}");
    } else {
        let timeouts = result.unwrap();
        assert_eq!(
            format!(
                "{},{},{}",
                timeouts.batch().as_millis(),
                timeouts.statement().as_millis(),
                timeouts.lock().as_millis()
            ),
            expected
        );
        let saved = timeouts;
        std::env::set_var(KEYS[0], "200000");
        assert_eq!(timeouts, saved);
        assert_eq!(
            WriteTimeouts::from_env().unwrap().batch().as_millis(),
            200000
        );
    }
}

#[test]
fn missing_env_uses_library_defaults() {
    let defaults = WriteTimeouts::default();
    assert_eq!(defaults.batch(), Duration::from_secs(150));
    assert_eq!(defaults.statement(), Duration::from_secs(120));
    assert_eq!(defaults.lock(), Duration::from_secs(5));
    probe(&[], "150000,120000,5000");
}

#[test]
fn overrides_each_timeout_independently() {
    for (key, value, expected) in [
        (KEYS[0], "180000", "180000,120000,5000"),
        (KEYS[1], "90000", "150000,90000,5000"),
        (KEYS[2], "10000", "150000,120000,10000"),
    ] {
        probe(&[(key, value.into())], expected);
    }
    probe(
        &[
            (KEYS[0], "30".into()),
            (KEYS[1], "20".into()),
            (KEYS[2], "5".into()),
        ],
        "30,20,5",
    );
}

#[test]
fn invalid_env_never_falls_back() {
    for key in KEYS {
        for value in [
            "",
            " ",
            "secret-invalid-value",
            "-1",
            "1.5",
            "0",
            "2147483648",
            "18446744073709551616",
        ] {
            probe(&[(key, value.into())], &format!("error:{key}"));
        }
    }
}

#[test]
fn env_requires_strict_timeout_order() {
    for (key, value) in [
        (KEYS[0], "120000"),
        (KEYS[1], "150000"),
        (KEYS[1], "5000"),
        (KEYS[2], "120000"),
    ] {
        probe(&[(key, value.into())], "error:DATA_SAVE_LOCK_TIMEOUT_MS < DATA_SAVE_STATEMENT_TIMEOUT_MS < DATA_SAVE_BATCH_TIMEOUT_MS");
    }
}

#[cfg(unix)]
#[test]
fn non_unicode_env_is_invalid() {
    for key in KEYS {
        probe(
            &[(key, OsString::from_vec(vec![0xff]))],
            &format!("error:{key}"),
        );
    }
}

#[test]
fn explicit_constructor_validates_boundaries() {
    assert!(WriteTimeouts::new(
        Duration::from_millis(3),
        Duration::from_millis(2),
        Duration::from_millis(1)
    )
    .is_ok());
    let max = i32::MAX as u64;
    assert!(WriteTimeouts::new(
        Duration::from_millis(max),
        Duration::from_millis(max - 1),
        Duration::from_millis(max - 2)
    )
    .is_ok());
    assert!(WriteTimeouts::new(Duration::ZERO, Duration::ZERO, Duration::ZERO).is_err());
    assert!(WriteTimeouts::new(
        Duration::from_millis(max + 1),
        Duration::from_secs(120),
        Duration::from_secs(5)
    )
    .is_err());
    assert!(WriteTimeouts::new(
        Duration::from_secs(3),
        Duration::from_secs(2),
        Duration::from_nanos(1)
    )
    .is_err());
}
