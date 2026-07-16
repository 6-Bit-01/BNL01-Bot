import os, sqlite3, unittest
from datetime import datetime, timezone, timedelta

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments

class MomentEngineV1Tests(unittest.TestCase):
    def setUp(self):
        os.environ["BNL_MEMORY_LEDGER_SHADOW_ENABLED"]="1"
        os.environ["BNL_MOMENT_ENGINE_SHADOW_ENABLED"]="1"
        self.conn=sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn); moments.ensure_moment_schema(self.conn)
    def tearDown(self): self.conn.close()
    def add(self,row,user,role,text,guild=1,chan=10,policy="sealed_test",mins=0,name=None):
        ts=(datetime(2026,1,1,tzinfo=timezone.utc)+timedelta(minutes=mins)).isoformat()
        r=ledger.shadow_conversation_row(self.conn,row_id=row,user_id=user,user_name=name or f"U{user}",guild_id=guild,role=role,content=text,channel_policy=policy,channel_id=chan,channel_name=f"c{chan}",route_mode="normal_chat",observed_at=ts)
        moments.observe_ledger_entry(self.conn,r.entry_id)
        return r
    def finalize_all(self): return moments.sweep_expired_windows(self.conn, now=datetime(2026,1,1,0,10,tzinfo=timezone.utc).isoformat())
    def test_two_humans_create_shared_activity_and_lineage_resolves(self):
        self.add(1,1,"user","red synth riff sounds huge")
        self.add(2,2,"user","red synth riff needs a drum answer")
        self.add(3,1,"user","yes the red synth and drums lock together")
        self.finalize_all()
        self.assertEqual(self.conn.execute("SELECT qualification_type FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()[0],"shared_activity")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_ledger_entries WHERE entry_type='shared_moment'").fetchone()[0],1)
        self.assertEqual(moments.build_moment_evaluation_report(self.conn,guild_id=1)["dangling_lineage_targets"],0)
    def test_one_human_and_bnl_conversational_moment(self):
        self.add(1,1,"user","remember this number: 731946")
        self.add(2,1,"model","I can track that as context")
        self.add(3,1,"user","also remember this number: 284517")
        self.add(4,1,"model","Noted")
        self.add(5,1,"user","what numbers did I ask you to remember?")
        self.finalize_all()
        row=self.conn.execute("SELECT qualification_type,summary FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()
        self.assertEqual(row[0],"conversational"); self.assertIn("731946",row[1]); self.assertIn("284517",row[1])
    def test_correction_marks_affected_moment_needs_review(self):
        r1=self.add(1,1,"user","remember this number: 8")
        self.add(2,1,"model","ok")
        self.add(3,1,"user","actually remember this number: 9 too")
        self.finalize_all(); self.assertEqual(moments.handle_source_correction(self.conn,r1.entry_id),1)
        self.assertEqual(self.conn.execute("SELECT lifecycle_status FROM memory_moment_windows").fetchone()[0],"needs_review")
    def test_bnl_only_and_low_signal_rejected(self):
        self.add(1,1,"model","hello") ; self.add(2,1,"model","ok") ; self.finalize_all()
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()[0],0)
        self.add(3,2,"user","hi",mins=3); self.add(4,2,"user","ok",mins=3); self.finalize_all()
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()[0],0)
    def test_punctuation_alone_does_not_qualify_but_burst_salience(self):
        self.add(1,1,"user","!!!!!"); self.add(2,2,"user","????"); self.add(3,1,"user","!!!!!"); self.finalize_all()
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()[0],0)
        self.add(10,1,"user","crystal bass patch opened in the bridge",mins=3)
        self.add(11,2,"user","crystal bass patch should answer the vocal",mins=3)
        self.add(12,3,"user","crystal bass patch made the room react",mins=3)
        self.finalize_all(); self.assertGreater(self.conn.execute("SELECT salience FROM memory_moment_windows WHERE lifecycle_status='finalized'").fetchone()[0],0.4)
    def test_topic_shift_channels_guilds_visibility_and_duplicates(self):
        r=self.add(1,1,"user","blue drums pattern is steady")
        moments.observe_ledger_entry(self.conn,r.entry_id)
        self.add(2,2,"user","blue drums pattern gets louder")
        self.add(3,1,"user","totally different pizza oven topic")
        self.add(4,3,"user","blue drums separate channel",chan=20)
        self.add(5,4,"user","blue drums separate guild",guild=2)
        self.add(6,5,"user","blue drums public visibility",policy="public_home")
        self.finalize_all()
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM memory_moment_members WHERE ledger_entry_id=?",(r.entry_id,)).fetchone()[0],1)
        report=moments.build_moment_evaluation_report(self.conn)
        self.assertEqual(report["cross_guild_violations"],0); self.assertEqual(report["cross_channel_violations"],0); self.assertEqual(report["incompatible_visibility_violations"],0)
    def test_restart_recovery_renderer_and_eval_detects_violations(self):
        self.add(1,1,"user","remember this number: 12345")
        mid=self.conn.execute("SELECT moment_id FROM memory_moment_windows WHERE lifecycle_status='open'").fetchone()[0]
        moments.sweep_expired_windows(self.conn, now=datetime(2026,1,1,0,3,tzinfo=timezone.utc).isoformat())
        moments.finalize_moment(self.conn,mid)
        rendered=moments.render_shadow_moment_context(self.conn,guild_id=1,channel_id=10,participant_key="discord_user:1",visibility="sealed_test",topic_text="number",token_budget=20)
        self.assertIn("Derived moment context",rendered)
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES('x',1,'derived_from','missing','now')")
        self.assertGreater(moments.build_moment_evaluation_report(self.conn)["dangling_lineage_targets"],0)

if __name__ == "__main__": unittest.main()
