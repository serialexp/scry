//! Process shutdown fan-out for long-running daemon roles.
//!
//! Kubernetes sends SIGTERM when terminating a pod, while local operators
//! commonly use Ctrl-C (SIGINT). One signal is captured here and broadcast over
//! a watch channel so every listener in a daemon observes the same shutdown.

use tokio::sync::watch;
use tracing::{info, warn};

/// Install SIGINT/SIGTERM handlers and return a clonable shutdown receiver.
///
/// The signal task owns the sender. When either signal arrives it publishes
/// `true`; if handler setup fails on Unix we retain SIGINT handling rather than
/// accidentally treating setup failure as a shutdown request.
pub fn channel() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        info!("shutdown signal received");
        let _ = tx.send(true);
    });
    rx
}

/// Wait until the channel has observed shutdown. The initial value is checked
/// before awaiting a change so a late subscriber cannot miss the notification.
pub async fn wait(mut rx: watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Resolve on SIGINT or, on Unix, SIGTERM (the Kubernetes termination signal).
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!(error = %e, "failed to install SIGINT handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                warn!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::wait;
    use tokio::sync::watch;

    #[tokio::test]
    async fn wait_observes_notification_after_subscribing() {
        let (tx, rx) = watch::channel(false);
        let waiter = tokio::spawn(wait(rx));
        tx.send(true).unwrap();
        waiter.await.unwrap();
    }

    #[tokio::test]
    async fn wait_observes_notification_before_subscribing() {
        let (tx, rx) = watch::channel(false);
        tx.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(50), wait(rx))
            .await
            .expect("late subscriber should return immediately");
    }
}
