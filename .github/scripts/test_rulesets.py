"""Exercise reconciliation without writing to GitHub."""

import copy
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import sync_rulesets as rulesets

REPO = "foyer-rs/foyer"


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.configs = rulesets.load_configs(rulesets.DEFAULT_DIRECTORY)
        self.ruleset_id, self.desired = self.configs[0]
        self.remote = copy.deepcopy(self.desired)
        self.remote.update(source_type="Repository", source=REPO)
        self.output = contextlib.redirect_stdout(io.StringIO())
        self.output.__enter__()
        self.addCleanup(self.output.__exit__, None, None, None)

    def changed_remote(self):
        remote = copy.deepcopy(self.remote)
        remote["name"] = "previous name"
        return remote

    @patch.object(rulesets, "api")
    def test_dry_run_never_writes(self, api):
        api.return_value = self.changed_remote()
        rulesets.sync(self.configs, REPO)
        self.assertTrue(all(len(call.args) == 1 for call in api.call_args_list))

    @patch.object(rulesets, "api")
    def test_unchanged_is_noop_even_with_reordered_rules(self, api):
        self.remote["rules"].reverse()
        api.return_value = self.remote
        rulesets.sync(self.configs, REPO, apply=True)
        self.assertEqual(api.call_count, 1)

    @patch.object(rulesets, "api")
    def test_update_preserves_complete_payload_and_verifies(self, api):
        previous = self.changed_remote()
        api.side_effect = [previous, previous, self.remote, self.remote]
        rulesets.sync(self.configs, REPO, apply=True)
        self.assertEqual(api.call_count, 4)
        self.assertEqual(api.call_args_list[2].args[1], self.desired)
        self.assertEqual(len(api.call_args_list[3].args), 1)

    @patch.object(rulesets, "api")
    def test_hidden_bypass_allowed_only_for_preview(self, api):
        del self.remote["bypass_actors"]
        api.return_value = self.remote
        rulesets.sync(self.configs, REPO)
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            rulesets.sync(self.configs, REPO, apply=True)
        self.assertTrue(all(len(call.args) == 1 for call in api.call_args_list))

    @patch.object(rulesets, "api")
    def test_foreign_source_rejected(self, api):
        self.remote["source_type"] = "Organization"
        api.return_value = self.remote
        with self.assertRaisesRegex(ValueError, "different source"):
            rulesets.sync(self.configs, REPO, apply=True)
        self.assertEqual(api.call_count, 1)

    @patch.object(rulesets, "api")
    def test_concurrent_edit_aborts_before_write(self, api):
        api.side_effect = [self.changed_remote(), self.remote]
        with self.assertRaisesRegex(ValueError, "changed during planning"):
            rulesets.sync(self.configs, REPO, apply=True)
        self.assertTrue(all(len(call.args) == 1 for call in api.call_args_list))

    @patch.object(rulesets, "api")
    def test_verification_failure_is_not_success(self, api):
        api.return_value = self.changed_remote()
        with self.assertRaisesRegex(ValueError, "verification failed"):
            rulesets.sync(self.configs, REPO, apply=True)

    def test_empty_directory_and_duplicate_ids_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with self.assertRaisesRegex(ValueError, "No ruleset"):
                rulesets.load_configs(directory)
            payload = json.dumps(dict(id=self.ruleset_id, **self.desired))
            (directory / "a.json").write_text(payload)
            (directory / "b.json").write_text(payload)
            with self.assertRaisesRegex(ValueError, "unique positive"):
                rulesets.load_configs(directory)


if __name__ == "__main__":
    unittest.main()
