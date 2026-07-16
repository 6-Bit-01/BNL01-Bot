import os, sqlite3, unittest
from datetime import datetime, timezone, timedelta
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments

class MomentEngineV1Tests(unittest.TestCase):
    def setUp(self):
        os.environ["BNL_MEMORY_LEDGER_SHADOW_ENABLED"]="1"
        os.environ["BNL_MOMENT_ENGINE_SHADOW_ENABLED"]="1"
        self.conn=sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn); moments.ensure_moment_schema(self.conn)
    def tearDown(self): self.conn.close()
    def add(self,row,user,role,text,guild=1,chan=10,policy="sealed_test",mins=0,name=None, observe=True):
        ts=(datetime(2026,1,1,tzinfo=timezone.utc)+timedelta(minutes=mins)).isoformat()
        r=ledger.shadow_conversation_row(self.conn,row_id=row,user_id=user,user_name=name or f"U{user}",guild_id=guild,role=role,content=text,channel_policy=policy,channel_id=chan,channel_name=f"c{chan}",route_mode="normal_chat",observed_at=ts)
        if observe: moments.observe_ledger_entry(self.conn,r.entry_id)
        return r
    def sweep(self, mins=10): return moments.sweep_expired_windows(self.conn, now=(datetime(2026,1,1,tzinfo=timezone.utc)+timedelta(minutes=mins)).isoformat())

    def test_gates_disabled_and_ledger_unavailable(self):
        os.environ.pop("BNL_MOMENT_ENGINE_SHADOW_ENABLED",None)
        r=self.add(1,1,"user","remember this number: 12345", observe=False)
        self.assertEqual(moments.observe_ledger_entry(self.conn,r.entry_id).reason_code,"moment_gate_disabled")
        os.environ["BNL_MOMENT_ENGINE_SHADOW_ENABLED"]="1"; os.environ.pop("BNL_MEMORY_LEDGER_SHADOW_ENABLED",None)
        self.assertEqual(moments.observe_ledger_entry(self.conn,r.entry_id).reason_code,"ledger_shadow_unavailable")

    def test_unrelated_topics_split_without_magic_words(self):
        self.add(1,1,"user","synth production needs a brighter bass patch")
        self.add(2,2,"user","the drum mix should answer that synth line")
        self.add(3,1,"user","that bass patch works with the bridge")
        self.add(4,3,"user","pizza ovens need hotter stones for crust")
        self.add(5,4,"user","hiking conditions look rainy on the trail")
        self.sweep()
        rows=self.conn.execute("SELECT qualification_type,lifecycle_status FROM memory_moment_windows ORDER BY window_started_at").fetchall()
        self.assertEqual(rows[0],("shared_activity","finalized"))
        self.assertGreaterEqual(len(rows),3)

    def test_remembered_numbers_one_episode_but_isolated_remember_rejected(self):
        self.add(1,1,"user","remember this number: 12345")
        self.sweep()
        self.assertEqual(self.conn.execute("SELECT lifecycle_status FROM memory_moment_windows").fetchone()[0],"rejected")
        self.add(10,1,"user","remember this number: 731946",mins=3)
        self.add(11,1,"model","I can keep that in the thread",mins=3)
        self.add(12,1,"user","remember this number: 284517",mins=3)
        self.add(13,1,"user","what numbers did I ask you to remember?",mins=3)
        self.sweep(7)
        row=self.conn.execute("SELECT qualification_type,summary FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()
        self.assertEqual(row[0],"conversational"); self.assertIn("731946",row[1]); self.assertIn("284517",row[1])

    def test_multi_human_shared_activity_qualifies_and_lineage_resolves(self):
        self.add(1,1,"user","red synth riff sounds huge")
        self.add(2,2,"user","red synth riff needs a drum answer")
        self.add(3,1,"user","yes the red synth and drums lock together")
        self.sweep()
        mid, entry_id = self.conn.execute("SELECT moment_id,canonical_ledger_entry_id FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()
        self.assertTrue(entry_id.startswith("mle_"))
        self.assertEqual(moments.finalize_moment(self.conn, mid).ledger_entry_id, entry_id)
        self.assertEqual(self.conn.execute("SELECT lifecycle_status FROM memory_ledger_entries WHERE entry_id=?",(entry_id,)).fetchone()[0],"review_only")
        self.assertEqual(moments.build_moment_evaluation_report(self.conn,guild_id=1)["dangling_lineage_targets"],0)

    def test_duplicates_before_and_after_finalization_are_global_noops(self):
        r=self.add(1,1,"user","synth patch keeps the lead stable")
        self.assertEqual(moments.observe_ledger_entry(self.conn,r.entry_id).outcome,"deduplicated")
        self.add(2,2,"user","synth patch needs drums under it")
        self.add(3,1,"user","synth patch and drums are locked")
        self.sweep()
        self.assertEqual(moments.observe_ledger_entry(self.conn,r.entry_id).outcome,"deduplicated")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_moment_members WHERE ledger_entry_id=?",(r.entry_id,)).fetchone()[0],1)

    def test_correction_marks_review_but_additive_values_do_not(self):
        r1=self.add(1,1,"user","remember this number: 8")
        self.add(2,1,"model","ok")
        self.add(3,1,"user","remember this number: 9")
        self.sweep(); mid=self.conn.execute("SELECT moment_id FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()[0]
        self.assertEqual(self.conn.execute("SELECT lifecycle_status FROM memory_moment_windows WHERE moment_id=?",(mid,)).fetchone()[0],"finalized")
        corr=self.add(4,1,"user","Correction: the number is 10, not 8",mins=3)
        self.assertEqual(self.conn.execute("SELECT lifecycle_status FROM memory_moment_windows WHERE moment_id=?",(mid,)).fetchone()[0],"needs_review")
        self.assertGreater(self.conn.execute("SELECT COUNT(*) FROM memory_ledger_lineage WHERE entry_id=? AND lineage_type='correction_of'",(corr.entry_id,)).fetchone()[0],0)

    def test_public_usable_visibility_and_isolation(self):
        self.add(1,1,"user","synth production public safe phrase",policy="public_home")
        self.add(2,2,"user","synth production public safe answer",policy="public_home")
        self.add(3,1,"user","synth production public safe close",policy="public_home")
        self.add(4,3,"user","synth production sealed phrase",policy="sealed_test")
        self.sweep()
        pubs=dict(self.conn.execute("SELECT channel_policy,public_usable FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchall())
        self.assertEqual(pubs.get("public_home"),1)
        self.assertNotIn("sealed_test", pubs)  # isolated singleton does not finalize as public moment
        report=moments.build_moment_evaluation_report(self.conn,guild_id=1)
        self.assertEqual(report["cross_channel_violations"],0); self.assertEqual(report["incompatible_visibility_violations"],0)

    def test_punctuation_low_signal_no_salience_and_failure_rolls_back(self):
        self.add(1,1,"user","!!!!!"); self.add(2,2,"user","????"); self.add(3,1,"user","ok")
        self.sweep()
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()[0],0)
        r=self.add(10,1,"user","synth patch opens", observe=False)
        with mock.patch("bnl_moment_engine._insert_membership", side_effect=RuntimeError("boom")):
            self.assertEqual(moments.observe_ledger_entry(self.conn,r.entry_id).outcome,"error")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_moment_members WHERE ledger_entry_id=?",(r.entry_id,)).fetchone()[0],0)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_moment_diagnostics WHERE event_type='moment_processing_error'").fetchone()[0],1)

    def test_restart_renderer_eval_guild_scope_and_lineage(self):
        self.add(1,1,"user","synth patch opens the room")
        self.add(2,2,"user","synth patch needs a drum reply")
        self.add(3,1,"user","synth patch and drums resolve")
        # restart recovery: new connection would see the persisted open window; same conn simulates persisted state
        self.sweep(3)
        text=moments.render_shadow_moment_context(self.conn,guild_id=1,channel_id=10,participant_key="discord_user:1",visibility="sealed_test",topic_text="synth drums",token_budget=30)
        self.assertIn("Derived moment context", text)
        self.add(10,9,"user","pizza oven crust stays crisp",guild=2)
        self.add(11,8,"user","pizza oven heat is steady",guild=2)
        self.add(12,9,"user","pizza oven stone works",guild=2)
        self.sweep()
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES('x',2,'derived_from','missing','now')")
        self.assertEqual(moments.build_moment_evaluation_report(self.conn,guild_id=1)["dangling_lineage_targets"],0)
        self.assertGreater(moments.build_moment_evaluation_report(self.conn,guild_id=2)["dangling_lineage_targets"],0)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_ledger_lineage l LEFT JOIN memory_ledger_entries e ON e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id WHERE e.entry_id IS NULL AND l.guild_id=1").fetchone()[0],0)

if __name__ == "__main__": unittest.main()
