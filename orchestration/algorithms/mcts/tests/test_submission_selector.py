from __future__ import annotations

import unittest

from xgovernor_mcts.engine.submission_selector import _parse_winner


def _payload(winner: str) -> str:
    labels = ["candidate_01", "candidate_02"]
    evaluations = [
        {
            "candidate": label,
            "eligible": True,
            "coverage": [],
            "task_alignment": 34 if label == winner else 20,
            "root_cause": 24 if label == winner else 15,
            "regression_safety": 24 if label == winner else 15,
            "implementation_quality": 14 if label == winner else 10,
            "fatal_flaws": [],
            "reason": "ok",
        }
        for label in labels
    ]
    import json
    return json.dumps({"requirements": [], "evaluations": evaluations})


class SubmissionSelectorParsingTests(unittest.TestCase):
    def test_parses_plain_json(self) -> None:
        winner = _parse_winner(_payload("candidate_02"), {"candidate_01", "candidate_02"})
        self.assertEqual(winner, "candidate_02")

    def test_parses_fenced_json(self) -> None:
        raw = "```json\n" + _payload("candidate_01") + "\n```"
        winner = _parse_winner(raw, {"candidate_01", "candidate_02"})
        self.assertEqual(winner, "candidate_01")

    def test_ignores_braces_in_reasoning_before_final_json(self) -> None:
        raw = (
            "Let me analyze.\n"
            'static const char *OPTIONS[] = {"listening_ports", "8081"};\n'
            + _payload("candidate_02")
        )
        winner = _parse_winner(raw, {"candidate_01", "candidate_02"})
        self.assertEqual(winner, "candidate_02")


if __name__ == "__main__":
    unittest.main()
