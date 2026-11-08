//! Minimal lifecycle contract shared by provider sessions.

use async_trait::async_trait;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Bounded-session lifecycle handle owned by the runtime or a host.
///
/// voice-live cannot force cleanup of arbitrary host tasks, so this is a
/// contract implementations must follow rather than a guarantee the trait
/// can enforce. The runtime verifies its own adapters and fakes against it.
#[async_trait]
pub trait ProviderSessionControl: Send {
    /// Requests immediate cancellation.
    ///
    /// Must not perform I/O and must not await: it only signals.
    fn cancel(&self);

    /// Closes provider-owned workers, joining them within a bounded time.
    ///
    /// Idempotent: closing an already-closed session succeeds immediately
    /// and must never hang.
    async fn close(&mut self) -> anyhow::Result<()>;
}

pub(crate) struct OwnedProviderTasks {
    pub token: CancellationToken,
    pub tasks: Vec<JoinHandle<()>>,
}

impl Drop for OwnedProviderTasks {
    fn drop(&mut self) {
        self.token.cancel();
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[async_trait]
impl ProviderSessionControl for OwnedProviderTasks {
    fn cancel(&self) {
        self.token.cancel();
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.token.cancel();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(1_000);
        while let Some(task) = self.tasks.last_mut() {
            if tokio::time::timeout_at(deadline, &mut *task).await.is_err() {
                tracing::warn!("provider worker exceeded cleanup deadline; aborting");
                task.abort();
                let _ = task.await;
            }
            self.tasks.pop();
        }
        Ok(())
    }
}

#[cfg(feature = "cartesia")]
pub(crate) fn cartesia_selector(value: &str, primary_only: bool) -> anyhow::Result<String> {
    let value = value.trim();
    anyhow::ensure!(
        !value.is_empty() && value.len() <= 63,
        "language selector must contain 1 to 63 bytes"
    );
    let normalized = match value.to_ascii_lowercase().as_str() {
        "korean" => "ko".to_owned(),
        "english" => "en".to_owned(),
        _ => value.replace('_', "-"),
    };
    let mut parts = normalized.split('-');
    let primary = parts.next().expect("nonempty selector");
    anyhow::ensure!(
        (2..=8).contains(&primary.len())
            && primary.bytes().all(|b| b.is_ascii_alphabetic())
            && !primary.eq_ignore_ascii_case("auto"),
        "invalid primary language selector"
    );
    anyhow::ensure!(
        parts
            .all(|p| !p.is_empty() && p.len() <= 8 && p.bytes().all(|b| b.is_ascii_alphanumeric())),
        "invalid language subtag"
    );
    Ok(if primary_only {
        primary.to_ascii_lowercase()
    } else {
        normalized
    })
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "cartesia")]
    #[test]
    fn selectors_are_syntax_not_a_language_registry() {
        for (input, primary, full) in [
            ("fr-CA", "fr", "fr-CA"),
            ("pt-BR", "pt", "pt-BR"),
            ("zh-Hant-TW", "zh", "zh-Hant-TW"),
            ("yue", "yue", "yue"),
            ("haw", "haw", "haw"),
            ("zz_ZZ", "zz", "zz-ZZ"),
            ("English", "en", "en"),
        ] {
            assert_eq!(super::cartesia_selector(input, true).unwrap(), primary);
            assert_eq!(super::cartesia_selector(input, false).unwrap(), full);
        }
        for input in [
            "", " ", "auto", "-fr", "fr-", "fr--CA", "en&x=y", "en?x", "en\nUS", "a",
        ] {
            assert!(super::cartesia_selector(input, false).is_err(), "{input:?}");
        }
    }
    #[cfg(any(feature = "cartesia", feature = "together", feature = "qwen"))]
    #[test]
    fn websocket_tls_can_select_a_crypto_provider() {
        let _builder = rustls::ClientConfig::builder();
        assert!(
            !rustls::crypto::CryptoProvider::get_default()
                .expect("crypto provider selected")
                .cipher_suites
                .is_empty()
        );
    }
}
