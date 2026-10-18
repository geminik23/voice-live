use super::ClaimClass;
use crate::interaction::ExpectedAnswerType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeechMode {
    #[default]
    Strict,
    Legacy,
}

/// Bounded executor observation, not a business success assertion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationProof {
    pub record_id: String,
    pub call_id: String,
    pub operation: String,
    pub source: Value,
    pub arguments: Value,
    pub output: String,
    pub executed: bool,
    pub success: bool,
    pub timed_out: bool,
    pub cancelled: bool,
    pub output_truncated: bool,
}

impl InvocationProof {
    pub fn bounded(&self) -> bool {
        !self.record_id.is_empty()
            && self.record_id.len() <= 128
            && !self.call_id.is_empty()
            && self.call_id.len() <= 256
            && !self.operation.is_empty()
            && self.operation.len() <= 256
            && self.arguments.to_string().len() <= 16_384
            && self.output.len() <= 16_384
            && self.source.to_string().len() <= 1024
    }

    pub fn usable(&self) -> bool {
        self.bounded()
            && !self.source.is_null()
            && self.executed
            && self.success
            && !self.timed_out
            && !self.cancelled
            && !self.output_truncated
    }
}

/// Trusted application resources never interpolate arbitrary model prose.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResponseResource {
    Prompt {
        text: String,
        #[serde(default)]
        question: Option<ExpectedAnswerType>,
    },
    ToolOutcome {
        text: String,
        operation: String,
        class: ClaimClass,
        receipt_fields: BTreeMap<String, Value>,
        bindings: Vec<String>,
        #[serde(default)]
        argument_defaults: BTreeMap<String, Value>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Proposal {
    response: String,
    #[serde(default)]
    call_id: Option<String>,
    #[serde(default)]
    arguments: Value,
}

pub struct RenderedReply {
    pub text: String,
    pub class: ClaimClass,
    pub question: Option<ExpectedAnswerType>,
    pub records: Vec<String>,
}

/// Trusted host boundary: validates domain outcomes and renders bounded meaning without I/O.
/// Implementations must not pass through unvalidated model text, labels, or template arguments.
pub trait SpeechRenderer: Send + Sync {
    fn render(&self, proposal: &str, proofs: &[InvocationProof]) -> anyhow::Result<RenderedReply>;
}

pub struct ConfiguredRenderer {
    resources: BTreeMap<String, ResponseResource>,
}

impl ConfiguredRenderer {
    pub fn new(resources: &BTreeMap<String, ResponseResource>) -> anyhow::Result<Self> {
        anyhow::ensure!(resources.len() <= 128, "too many response resources");
        for (name, resource) in resources {
            anyhow::ensure!(
                !name.is_empty() && name.len() <= 128,
                "invalid response resource name"
            );
            let text = match resource {
                ResponseResource::Prompt { text, .. } => text,
                ResponseResource::ToolOutcome {
                    text,
                    operation,
                    class,
                    receipt_fields,
                    bindings,
                    argument_defaults,
                } => {
                    anyhow::ensure!(
                        argument_defaults.len() <= 32
                            && argument_defaults
                                .values()
                                .all(|v| v.to_string().len() <= 1024),
                        "argument defaults exceed budget"
                    );
                    anyhow::ensure!(
                        !operation.is_empty() && operation.len() <= 256,
                        "invalid response operation"
                    );
                    anyhow::ensure!(
                        matches!(class, ClaimClass::Factual | ClaimClass::Transactional),
                        "tool outcome must be factual or transactional"
                    );
                    anyhow::ensure!(
                        !receipt_fields.is_empty()
                            && !bindings.is_empty()
                            && bindings.len() <= 32
                            && receipt_fields.len() <= 32,
                        "tool outcome needs bounded receipt checks and target bindings"
                    );
                    anyhow::ensure!(
                        receipt_fields.values().all(|v| v.to_string().len() <= 1024),
                        "receipt check is too large"
                    );
                    text
                }
            };
            anyhow::ensure!(
                !text.trim().is_empty() && text.len() <= 4096,
                "response text must contain 1 to 4096 bytes"
            );
        }
        Ok(Self {
            resources: resources.clone(),
        })
    }
}

impl SpeechRenderer for ConfiguredRenderer {
    fn render(&self, proposal: &str, proofs: &[InvocationProof]) -> anyhow::Result<RenderedReply> {
        anyhow::ensure!(proposal.len() <= 16_384, "proposal too large");
        let proposal: Proposal = serde_json::from_str(proposal)?;
        let resource = self
            .resources
            .get(&proposal.response)
            .ok_or_else(|| anyhow::anyhow!("unknown speech meaning"))?;
        match resource {
            ResponseResource::Prompt { text, question } => {
                anyhow::ensure!(
                    proposal.call_id.is_none() && proposal.arguments.is_null(),
                    "prompt takes no model arguments"
                );
                Ok(RenderedReply {
                    text: text.clone(),
                    class: ClaimClass::Phatic,
                    question: *question,
                    records: vec![],
                })
            }
            ResponseResource::ToolOutcome {
                text,
                operation,
                class,
                receipt_fields,
                bindings,
                argument_defaults,
            } => {
                let call_id = proposal
                    .call_id
                    .ok_or_else(|| anyhow::anyhow!("missing invocation"))?;
                let matching: Vec<_> = proofs
                    .iter()
                    .filter(|p| p.call_id == call_id && p.operation == *operation)
                    .collect();
                anyhow::ensure!(matching.len() == 1, "ambiguous invocation outcome");
                let proof = matching[0];
                anyhow::ensure!(
                    proof.usable() && proof.arguments == proposal.arguments,
                    "unproven invocation or mismatched executed arguments"
                );
                let receipt: Value = serde_json::from_str(&proof.output)?;
                anyhow::ensure!(
                    receipt.is_object() && receipt.get("skipped").is_none(),
                    "invalid business receipt"
                );
                anyhow::ensure!(
                    receipt_fields
                        .iter()
                        .all(|(k, v)| receipt.get(k) == Some(v)),
                    "business outcome not confirmed"
                );
                anyhow::ensure!(
                    bindings.iter().all(|k| proof
                        .arguments
                        .get(k)
                        .or_else(|| argument_defaults.get(k))
                        .is_some_and(|v| !v.is_null() && receipt.get(k) == Some(v))),
                    "receipt target differs from executed arguments"
                );
                Ok(RenderedReply {
                    text: text.clone(),
                    class: *class,
                    question: None,
                    records: vec![proof.record_id.clone()],
                })
            }
        }
    }
}
