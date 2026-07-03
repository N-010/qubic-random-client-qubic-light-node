use std::fmt;
use std::io::{self, Write};
use std::sync::OnceLock;
use std::sync::mpsc::{SyncSender, sync_channel};

const LOG_QUEUE_CAPACITY: usize = 4_096;

enum Destination {
    Stdout,
    Stderr,
}

struct LogMessage {
    destination: Destination,
    text: String,
}

struct Logger {
    tx: SyncSender<LogMessage>,
}

impl Logger {
    fn enqueue(&self, destination: Destination, args: fmt::Arguments<'_>) {
        let _ = self.tx.try_send(LogMessage {
            destination,
            text: args.to_string(),
        });
    }
}

static LOGGER: OnceLock<Logger> = OnceLock::new();

pub(crate) fn init() {
    let _ = logger();
}

pub(crate) fn info(args: fmt::Arguments<'_>) {
    logger().enqueue(Destination::Stdout, args);
}

pub(crate) fn error(args: fmt::Arguments<'_>) {
    logger().enqueue(Destination::Stderr, args);
}

fn logger() -> &'static Logger {
    LOGGER.get_or_init(|| {
        let (tx, rx) = sync_channel::<LogMessage>(LOG_QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("lightnode-logger".to_string())
            .spawn(move || {
                while let Ok(message) = rx.recv() {
                    match message.destination {
                        Destination::Stdout => write_message(io::stdout().lock(), &message.text),
                        Destination::Stderr => write_message(io::stderr().lock(), &message.text),
                    }
                }
            })
            .expect("failed to start logging thread");
        Logger { tx }
    })
}

fn write_message(mut writer: impl Write, message: &str) {
    let _ = writeln!(writer, "{message}");
    let _ = writer.flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn full_log_queue_does_not_block_producer() {
        let (tx, _rx) = sync_channel(1);
        let logger = Logger { tx };
        logger.enqueue(Destination::Stdout, format_args!("first"));

        let started = Instant::now();
        logger.enqueue(Destination::Stderr, format_args!("dropped"));

        assert!(started.elapsed() < Duration::from_millis(100));
    }
}
