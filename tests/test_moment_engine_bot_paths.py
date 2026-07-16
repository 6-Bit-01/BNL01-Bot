import os, sqlite3, tempfile, unittest
os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")
import bnl01_bot, bnl_moment_engine as moments

class MomentEngineBotPathTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.NamedTemporaryFile(delete=False); self.tmp.close()
        self.old=bnl01_bot.DB_FILE; bnl01_bot.DB_FILE=self.tmp.name
        os.environ["BNL_MEMORY_LEDGER_SHADOW_ENABLED"]="1"; os.environ.pop("BNL_MOMENT_ENGINE_SHADOW_ENABLED",None)
        bnl01_bot.init_db()
    def tearDown(self):
        bnl01_bot.DB_FILE=self.old
    def rows(self,sql):
        c=sqlite3.connect(self.tmp.name); r=c.execute(sql).fetchall(); c.close(); return r
    def test_disabled_gate_creates_no_moment_windows(self):
        bnl01_bot.save_user_message(1,"A",1,"remember this number: 8",channel_policy="sealed_test",channel_id=10)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_moment_windows")[0][0],0)
    def test_enabled_gate_observes_after_ledger_write_and_failure_is_isolated(self):
        os.environ["BNL_MOMENT_ENGINE_SHADOW_ENABLED"]="1"
        bnl01_bot.save_user_message(1,"A",1,"remember this number: 731946",channel_policy="sealed_test",channel_id=10)
        bnl01_bot.save_model_message(1,1,"ok",channel_policy="sealed_test",channel_id=10)
        bnl01_bot.save_user_message(1,"A",1,"what numbers did I ask you to remember?",channel_policy="sealed_test",channel_id=10)
        self.assertGreaterEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_entries WHERE source_table='conversations'")[0][0],3)
        self.assertGreaterEqual(self.rows("SELECT COUNT(*) FROM memory_moment_windows")[0][0],1)
    def test_moment_gate_without_ledger_gate_skips_safely(self):
        os.environ.pop("BNL_MEMORY_LEDGER_SHADOW_ENABLED",None); os.environ["BNL_MOMENT_ENGINE_SHADOW_ENABLED"]="1"
        bnl01_bot.save_user_message(2,"B",1,"remember this number: 9",channel_policy="sealed_test",channel_id=10)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM memory_ledger_entries")[0][0],0)

if __name__ == "__main__": unittest.main()
