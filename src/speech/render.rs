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

/// How a named business outcome is checked against the turn's executed tool
/// records. The words spoken come from the model; this rule decides whether the
/// outcome it names actually happened.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeRule {
    pub operation: String,
    pub class: ClaimClass,
    pub receipt_fields: BTreeMap<String, Value>,
    pub bindings: Vec<String>,
    #[serde(default)]
    pub argument_defaults: BTreeMap<String, Value>,
}

/// A strict task-agent Final: the reply text, an optional outcome name, and
/// an optional answer-type hint for the last clause. A Final that is not a
/// JSON object is read as reply text with no outcome.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Proposal {
    text: String,
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default)]
    question: Option<ExpectedAnswerType>,
}

impl Proposal {
    fn parse(raw: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(raw.len() <= 16_384, "proposal too large");
        let trimmed = raw.trim();
        let proposal = if trimmed.starts_with('{') {
            serde_json::from_str(trimmed)?
        } else {
            Self {
                text: trimmed.to_string(),
                outcome: None,
                question: None,
            }
        };
        anyhow::ensure!(!proposal.text.trim().is_empty(), "empty reply text");
        Ok(proposal)
    }
}

pub struct RenderedReply {
    pub text: String,
    pub class: ClaimClass,
    pub question: Option<ExpectedAnswerType>,
    pub records: Vec<String>,
}

/// Trusted host boundary between a task-agent Final and speech, without I/O.
/// Reply text is model-generated; an implementation must not let a reply claim
/// a business outcome that the turn's executed tool records do not confirm.
pub trait SpeechRenderer: Send + Sync {
    fn render(&self, proposal: &str, proofs: &[InvocationProof]) -> anyhow::Result<RenderedReply>;
}

pub struct ConfiguredRenderer {
    outcomes: BTreeMap<String, OutcomeRule>,
}

impl ConfiguredRenderer {
    pub fn new(outcomes: &BTreeMap<String, OutcomeRule>) -> anyhow::Result<Self> {
        anyhow::ensure!(outcomes.len() <= 128, "too many outcome rules");
        for (name, rule) in outcomes {
            anyhow::ensure!(
                !name.is_empty() && name.len() <= 128,
                "invalid outcome name"
            );
            anyhow::ensure!(
                rule.argument_defaults.len() <= 32
                    && rule
                        .argument_defaults
                        .values()
                        .all(|v| v.to_string().len() <= 1024),
                "argument defaults exceed budget"
            );
            anyhow::ensure!(
                !rule.operation.is_empty() && rule.operation.len() <= 256,
                "invalid outcome operation"
            );
            anyhow::ensure!(
                matches!(rule.class, ClaimClass::Factual | ClaimClass::Transactional),
                "outcome class must be factual or transactional"
            );
            anyhow::ensure!(
                !rule.receipt_fields.is_empty()
                    && !rule.bindings.is_empty()
                    && rule.bindings.len() <= 32
                    && rule.receipt_fields.len() <= 32,
                "outcome needs bounded receipt checks and target bindings"
            );
            anyhow::ensure!(
                rule.receipt_fields
                    .values()
                    .all(|v| v.to_string().len() <= 1024),
                "receipt check is too large"
            );
        }
        Ok(Self {
            outcomes: outcomes.clone(),
        })
    }

    /// Every record of the rule's operation in this turn must be usable and
    /// confirm the receipt and its targets; one failed attempt rejects the claim.
    fn confirm(
        &self,
        rule: &OutcomeRule,
        proofs: &[InvocationProof],
    ) -> anyhow::Result<Vec<String>> {
        let matching: Vec<_> = proofs
            .iter()
            .filter(|p| p.operation == rule.operation)
            .collect();
        anyhow::ensure!(!matching.is_empty(), "no executed invocation for outcome");
        for proof in &matching {
            anyhow::ensure!(proof.usable(), "unproven invocation");
            let receipt: Value = serde_json::from_str(&proof.output)?;
            anyhow::ensure!(
                receipt.is_object() && receipt.get("skipped").is_none(),
                "invalid business receipt"
            );
            anyhow::ensure!(
                rule.receipt_fields
                    .iter()
                    .all(|(k, v)| receipt.get(k) == Some(v)),
                "business outcome not confirmed"
            );
            anyhow::ensure!(
                rule.bindings.iter().all(|k| proof
                    .arguments
                    .get(k)
                    .or_else(|| rule.argument_defaults.get(k))
                    .is_some_and(|v| !v.is_null() && receipt.get(k) == Some(v))),
                "receipt target differs from executed arguments"
            );
        }
        Ok(matching.iter().map(|p| p.record_id.clone()).collect())
    }
}

impl SpeechRenderer for ConfiguredRenderer {
    fn render(&self, proposal: &str, proofs: &[InvocationProof]) -> anyhow::Result<RenderedReply> {
        let proposal = Proposal::parse(proposal)?;
        let Some(name) = proposal.outcome else {
            return Ok(RenderedReply {
                text: proposal.text,
                class: ClaimClass::Phatic,
                question: proposal.question,
                records: vec![],
            });
        };
        let rule = self
            .outcomes
            .get(&name)
            .ok_or_else(|| anyhow::anyhow!("unknown outcome"))?;
        let records = self.confirm(rule, proofs)?;
        Ok(RenderedReply {
            text: proposal.text,
            class: rule.class,
            question: proposal.question,
            records,
        })
    }
}
