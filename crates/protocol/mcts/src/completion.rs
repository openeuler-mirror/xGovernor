use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyPlan {
    pub kind: String,
    pub state_id: String,
    pub base_state_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Candidate {
    pub candidate_id: String,
    pub state_id: String,
    pub score: f64,
    pub evaluation_version: String,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub evidence_id: String,
    pub kind: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionResult {
    pub status: String,
    pub summary: String,
    pub answer: String,
    pub stop_reason: String,
    pub selected_candidate_id: Option<String>,
    pub candidates: Vec<Candidate>,
    pub evidence: Vec<Evidence>,
    pub limitations: Vec<String>,
    pub apply_plan: Option<ApplyPlan>,
}

impl CompletionResult {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.status != "completed" {
            return Err(ProtocolError::InvalidMessage(
                "completion result status must be 'completed'".to_string(),
            ));
        }

        let candidate_ids: BTreeSet<&str> = self
            .candidates
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
            .collect();
        if candidate_ids.len() != self.candidates.len() {
            return Err(ProtocolError::InvalidMessage(
                "candidate ids must be unique".to_string(),
            ));
        }

        let evidence_ids: BTreeSet<&str> = self
            .evidence
            .iter()
            .map(|evidence| evidence.evidence_id.as_str())
            .collect();
        if evidence_ids.len() != self.evidence.len() {
            return Err(ProtocolError::InvalidMessage(
                "evidence ids must be unique".to_string(),
            ));
        }

        for candidate in &self.candidates {
            for reference in &candidate.evidence_refs {
                if !evidence_ids.contains(reference.as_str()) {
                    return Err(ProtocolError::InvalidMessage(format!(
                        "candidate {} references unknown evidence {}",
                        candidate.candidate_id, reference
                    )));
                }
            }
        }

        if let Some(selected_id) = &self.selected_candidate_id {
            let selected = self
                .candidates
                .iter()
                .find(|candidate| candidate.candidate_id == *selected_id)
                .ok_or_else(|| {
                    ProtocolError::InvalidMessage(
                        "selected candidate id does not resolve".to_string(),
                    )
                })?;
            if let Some(apply_plan) = &self.apply_plan {
                if apply_plan.state_id != selected.state_id {
                    return Err(ProtocolError::InvalidMessage(
                        "apply_plan state does not match selected candidate".to_string(),
                    ));
                }
            }
        } else if self.apply_plan.is_some() {
            return Err(ProtocolError::InvalidMessage(
                "apply_plan requires a selected candidate".to_string(),
            ));
        }

        Ok(())
    }
}
