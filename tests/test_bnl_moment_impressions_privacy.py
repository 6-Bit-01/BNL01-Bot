"""Actual existing deletion owners erase retained impression content, not just reads."""
import sqlite3
import unittest

import bnl_memory_governance as governance
import test_bnl_moment_impressions as fixtures


class MomentImpressionPrivacyTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.MomentImpressionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.conn = self.fixture.conn
        self.mid, self.roots, _ = self.fixture.ready()
        self.conn.commit()
        self.assertTrue(self.stored()[0])

    def stored(self):
        return self.conn.execute(
            'SELECT impression_payload,impression_source_digest,impression_projection_digest '
            'FROM memory_moment_windows WHERE moment_id=?', (self.mid,),
        ).fetchone()

    def test_exact_conversation_source_purge_erases_subjective_content(self):
        result = governance.purge_conversation_ledger_sources(
            self.conn, guild_id=1, source_row_ids=(1002,), reason='source_removed',
        )
        self.assertEqual(result['moment_windows_retracted'], 1)
        self.assertEqual(self.stored(), ('', '', ''))
        self.assertIsNone(self.fixture.read(self.mid))

    def test_complete_member_deletion_erases_subjective_content(self):
        result = governance.complete_delete_member_data(
            self.conn, guild_id=1, user_id=7, confirmation='DELETE MY BNL DATA 1',
        )
        self.assertTrue(result['ok'])
        self.assertEqual(self.stored(), ('', '', ''))
        self.assertIsNone(self.fixture.read(self.mid))

    def test_original_contribution_forget_scrubs_impression_in_existing_invalidation_path(self):
        governance._invalidate_contributions_for_ledger_entries(
            self.conn, guild_id=1, ledger_entry_ids=(self.roots[2],), lifecycle='forgotten',
        )
        self.assertEqual(self.stored(), ('', '', ''))
        self.assertIsNone(self.fixture.read(self.mid))

    def test_erasure_scope_and_legacy_schema_are_preserved(self):
        governance._scrub_moment_impressions(self.conn, guild_id=2, moment_ids=(self.mid,))
        self.assertTrue(self.stored()[0])
        old = sqlite3.connect(':memory:')
        self.addCleanup(old.close)
        old.execute('CREATE TABLE memory_moment_windows(moment_id TEXT, guild_id INTEGER, summary TEXT)')
        old.execute("INSERT INTO memory_moment_windows VALUES ('old',1,'Old factual gist')")
        governance._scrub_moment_impressions(old, guild_id=1, moment_ids=('old',))
        self.assertEqual(old.execute('SELECT * FROM memory_moment_windows').fetchall(),
                         [('old', 1, 'Old factual gist')])
        self.assertEqual(len(old.execute('PRAGMA table_info(memory_moment_windows)').fetchall()), 3)


if __name__ == '__main__':
    unittest.main()
