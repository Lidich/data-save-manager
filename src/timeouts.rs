use std::env;
use std::fmt;
use std::time::Duration;

const BATCH: &str = "DATA_SAVE_BATCH_TIMEOUT_MS";
const STATEMENT: &str = "DATA_SAVE_STATEMENT_TIMEOUT_MS";
const LOCK: &str = "DATA_SAVE_LOCK_TIMEOUT_MS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteTimeouts {
    batch: Duration,
    statement: Duration,
    lock: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutConfigError(&'static str);

impl fmt::Display for TimeoutConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid data save timeouts: {}", self.0)
    }
}

impl std::error::Error for TimeoutConfigError {}

impl Default for WriteTimeouts {
    fn default() -> Self {
        Self {
            batch: Duration::from_secs(150),
            statement: Duration::from_secs(120),
            lock: Duration::from_secs(5),
        }
    }
}

impl WriteTimeouts {
    pub fn new(
        batch: Duration,
        statement: Duration,
        lock: Duration,
    ) -> Result<Self, TimeoutConfigError> {
        for (key, duration) in [(BATCH, batch), (STATEMENT, statement), (LOCK, lock)] {
            if duration.is_zero()
                || duration.as_millis() > i32::MAX as u128
                || duration.subsec_nanos() % 1_000_000 != 0
            {
                return Err(TimeoutConfigError(key));
            }
        }
        if !(lock < statement && statement < batch) {
            return Err(TimeoutConfigError("require DATA_SAVE_LOCK_TIMEOUT_MS < DATA_SAVE_STATEMENT_TIMEOUT_MS < DATA_SAVE_BATCH_TIMEOUT_MS"));
        }
        Ok(Self {
            batch,
            statement,
            lock,
        })
    }

    pub fn from_env() -> Result<Self, TimeoutConfigError> {
        let defaults = Self::default();
        Self::new(
            read(BATCH, defaults.batch)?,
            read(STATEMENT, defaults.statement)?,
            read(LOCK, defaults.lock)?,
        )
    }

    pub fn batch(self) -> Duration {
        self.batch
    }
    pub fn statement(self) -> Duration {
        self.statement
    }
    pub fn lock(self) -> Duration {
        self.lock
    }
}

fn read(key: &'static str, default: Duration) -> Result<Duration, TimeoutConfigError> {
    match env::var(key) {
        Ok(value) => value
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|_| TimeoutConfigError(key)),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(TimeoutConfigError(key)),
    }
}
