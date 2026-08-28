use async_trait::async_trait;
pub use slg_domain::CredentialReference;
use slg_ports::SecretResolver;

#[derive(Debug, Clone, Copy, Default)]
pub struct EnvironmentSecretResolver;

#[async_trait]
impl SecretResolver for EnvironmentSecretResolver {
    async fn resolve(&self, reference: &CredentialReference) -> Result<String, String> {
        std::env::var(reference.environment_name())
            .map_err(|_| "configured credential environment variable is not set".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_valid_environment_references() {
        let reference = CredentialReference::parse("env:PROVIDER_API_KEY").unwrap();
        assert_eq!(reference.environment_name(), "PROVIDER_API_KEY");
        assert_eq!(reference.as_str(), "env:PROVIDER_API_KEY");
    }

    #[test]
    fn rejects_bare_names_literals_and_invalid_environment_names() {
        for reference in [
            "PROVIDER_API_KEY",
            "sk-live-literal-secret",
            "env:",
            "env:1INVALID",
            "env:INVALID-NAME",
        ] {
            assert!(
                CredentialReference::parse(reference).is_err(),
                "{reference}"
            );
        }
    }

    #[tokio::test]
    async fn resolver_rejects_literal_credential_without_echoing_it() {
        let literal = "sk-live-credential-that-must-not-leak";
        let error = CredentialReference::parse(literal).unwrap_err().to_string();
        assert!(error.contains("env:NAME"));
        assert!(!error.contains(literal));
    }

    #[tokio::test]
    async fn resolver_uses_the_validated_environment_name() {
        let reference = CredentialReference::parse("env:SLG_MISSING_TEST_CREDENTIAL").unwrap();
        let error = EnvironmentSecretResolver
            .resolve(&reference)
            .await
            .unwrap_err();
        assert_eq!(
            error,
            "configured credential environment variable is not set"
        );
    }
}
