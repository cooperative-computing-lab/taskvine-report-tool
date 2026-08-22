import unittest
from unittest import mock

from src.vine_parse.protocol import (
    CandidateClassifier,
    HANDLER_MASKS,
    HANDLER_NAMES,
    reference_candidate_mask,
)


SAMPLES = [
    "2026/08/19 12:00:00.00 vine_manager[1]vine: busy on worker 10.0.0.1:1 task 7",
    "2026/08/19 12:00:00.00 vine_manager[1]vine: puturl workerip://10.0.0.1/a cached",
    "2026/08/19 12:00:00.00 vine_manager[1]vine: Task 7 state change: READY (1) to RUNNING (2)",
    "2026/08/19 12:00:00.00 vine_manager[1]vine: worker 10.0.0.1:1 connected",
    "2026/08/19 12:00:00.00 vine_manager[1]vine: worker host (10.0.0.1:1) removed",
    "2026/08/19 12:00:00.00 vine_manager[1]vine: tx to worker task 7",
    "2026/08/19 12:00:00.00 vine_manager[1]vine: manager end",
    "ordinary debug text with no recognized protocol family",
    "embedded\x00worker connected",
]


class NativeParserTest(unittest.TestCase):
    def test_reference_classifier_covers_every_handler(self):
        examples = {
            "busy_on": "x busy on y",
            "puturl": "x puturl y",
            "cache_update": "x cache-update y",
            "task_state_change": "x state change: y",
            "unlink": "x unlink y",
            "worker_removed": "x worker removed y",
            "complete": "x complete y",
            "worker_received": "x received y",
            "receive_worker_info": "x info y",
            "cache_invalid": "x cache-invalid y",
            "worker_resources": "x resources y",
            "stdout": "x stdout y",
            "recovery_task": "x Submitted recovery task y",
            "put_file": "x put y",
            "failed_to_send_task": "x Failed to send task y",
            "transfer_port": "x transfer-port y",
            "exhausted_resources": "x exhausted resources y",
            "listening": "x listening on port y",
            "worker_connected": "x worker connected y",
            "manager_end": "x manager end y",
            "remove_instances": "x Removing instances of worker y",
            "kill_task": "x kill y",
            "added_dependency": "x added dependency y",
            "send_task_to_worker": "x tx to y",
        }
        self.assertEqual(tuple(examples), HANDLER_NAMES)
        for name, line in examples.items():
            with self.subTest(name=name):
                mask = reference_candidate_mask(line, line.split(" "))
                self.assertTrue(mask & HANDLER_MASKS[name])

    def test_native_and_reference_classifiers_are_identical(self):
        classifier = CandidateClassifier()
        if not classifier.available:
            self.skipTest("optional native classifier is not built")
        expected = [reference_candidate_mask(line, line.split(" ")) for line in SAMPLES]
        self.assertEqual(list(classifier.classify_batch(SAMPLES)), expected)

    def test_native_matches_reference_across_token_and_phrase_combinations(self):
        classifier = CandidateClassifier()
        if not classifier.available:
            self.skipTest("optional native classifier is not built")
        fragments = (
            "worker connected", "worker removed", "resources exhausted",
            "puturl_now", "cache-update", "state change:", "tx to",
            "Submitted recovery task", "manager end", "x\x00y", "café",
        )
        lines = [
            "prefix {} {} suffix".format(left, right)
            for left in fragments
            for right in fragments
        ]
        expected = [reference_candidate_mask(line, line.split(" ")) for line in lines]
        self.assertEqual(list(classifier.classify_batch(lines)), expected)

    def test_fallback_is_deliberately_non_filtering(self):
        classifier = CandidateClassifier()
        classifier._native = None
        records = list(classifier.classified_records([1, 2, 3]))
        self.assertEqual(records, [(1, None), (2, None), (3, None)])

    def test_native_path_has_an_operational_kill_switch(self):
        with mock.patch.dict("os.environ", {"TASKVINE_DISABLE_NATIVE_PARSER": "1"}):
            self.assertFalse(CandidateClassifier().available)


if __name__ == "__main__":
    unittest.main()
