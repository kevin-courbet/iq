use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitObjectFormat {
    Sha1,
    Sha256,
}

impl GitObjectFormat {
    pub fn parse(value: &str, label: &str) -> Result<Self> {
        match value {
            "sha1" => Ok(Self::Sha1),
            "sha256" => Ok(Self::Sha256),
            _ => anyhow::bail!("{label} reported unsupported Git object format {value:?}"),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sha1 => "sha1",
            Self::Sha256 => "sha256",
        }
    }

    pub const fn oid_length(self) -> usize {
        match self {
            Self::Sha1 => 40,
            Self::Sha256 => 64,
        }
    }

    pub fn zero_oid(self) -> String {
        "0".repeat(self.oid_length())
    }

    pub fn require_oid(self, value: &str, label: &str) -> Result<()> {
        if value.len() != self.oid_length() || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            anyhow::bail!(
                "{label} must be a full hexadecimal {} Git object ID",
                self.as_str()
            );
        }
        Ok(())
    }
}

impl std::fmt::Display for GitObjectFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
