"""Bulk source reads must equal the frozen scalar source fence, not skip it."""
from __future__ import annotations

import json
import sqlite3
import tracemalloc
import unittest
from unittest import mock

import bnl_relationship_engine as relationship
from tests import test_private_conversation_source_lookup as lookup


# Frozen before this change from 53d20e7. This owner is independent of any
# candidate query, batching helper or prepared-source validation branch.
_SCALAR_OWNER = '''
def _meaning_source(conn, entry_id, *, guild_id, user_id, private_channel_id=0):
    from bnl_memory_ledger import BNL_SUBJECT_KEY
    from bnl_moment_engine import _contains_sensitive_moment_source
    if not _table_exists(conn, "conversations") or not _table_exists(conn, "memory_ledger_entries"):
        return None
    cursor = conn.execute("""SELECT source_table,source_row_id,channel_policy,channel_id,
        visibility,public_usable,route_mode,lifecycle_status,source_role,normalized_value,
        predicate_key,subject_key,derived,projection,source_revision,observed_at
        FROM memory_ledger_entries WHERE entry_id=? AND guild_id=?""", (entry_id,guild_id))
    row = cursor.fetchone()
    if not row:
        return None
    entry = dict(zip((column[0] for column in cursor.description),row))
    private = (private_channel_id > 0 and entry["channel_policy"] == "sealed_test"
        and entry["channel_id"] == private_channel_id and entry["visibility"] == "sealed_test"
        and not entry["public_usable"])
    if (entry["source_table"] != "conversations" or not str(entry["source_row_id"]).isdigit()
        or (not private and entry["visibility"] not in {"public","public_safe"})
        or (not private and entry["channel_policy"] not in PUBLIC_POLICIES)
        or entry["route_mode"] not in RELATIONSHIP_LIVE_ROUTES
        or entry["lifecycle_status"] not in {"active","review_only"}
        or conn.execute("SELECT 1 FROM memory_ledger_lineage WHERE guild_id=? "
            "AND target_entry_id=? AND lineage_type IN ('correction_of','supersedes','retracts')",
            (guild_id,entry_id)).fetchone()):
        return None
    cursor = conn.execute("""SELECT id,role,content,channel_id,channel_policy,route_mode,timestamp
        FROM conversations WHERE id=? AND guild_id=? AND user_id=?""",
        (int(entry["source_row_id"]),guild_id,user_id))
    row = cursor.fetchone()
    if not row:
        return None
    original = dict(zip((column[0] for column in cursor.description),row))
    role,text = original["role"],str(original["content"] or "")
    if (role != entry["source_role"] or role not in {"user","model"}
        or original.get("channel_id") != entry["channel_id"]
        or original["channel_policy"] != entry["channel_policy"]
        or original["route_mode"] != entry["route_mode"]
        or not text.strip() or len(text)>6000 or text[:500] != entry["normalized_value"]
        or _contains_sensitive_moment_source(text,entry["predicate_key"])):
        return None
    if role == "user":
        if (entry["subject_key"] != subject_key_for_user(user_id) or entry["derived"]
            or entry["projection"] or (not private and not entry["public_usable"])
            or entry["lifecycle_status"] != "active"):
            return None
    elif entry["subject_key"] != BNL_SUBJECT_KEY:
        return None
    elif _table_exists(conn,"conversation_response_participants"):
        targets = {int(r[0]) for r in conn.execute(
            "SELECT user_id FROM conversation_response_participants WHERE guild_id=? AND conversation_row_id=?",
            (guild_id,original["id"]))}
        if targets and targets != {user_id}:
            return None
    return {"entry_id":entry_id,"row_id":original["id"],"role":role,"text":text,
        "timestamp":original["timestamp"],"channel_id":entry["channel_id"],
        "ledger":{key:entry[key] for key in ("subject_key","source_revision","source_role",
            "channel_policy","route_mode","visibility","public_usable","derived",
            "projection","lifecycle_status","observed_at")}}

def private_conversation_sources(conn, *, guild_id, user_id, channel_id):
    if channel_id<=0 or not _table_exists(conn,'memory_ledger_entries'):
        return ()
    roots=conn.execute("""SELECT e.entry_id FROM conversations c
        CROSS JOIN memory_ledger_entries e ON e.guild_id=c.guild_id
          AND e.source_table='conversations' AND e.source_row_id=CAST(c.id AS TEXT)
        WHERE c.guild_id=? AND c.user_id=? AND c.channel_policy='sealed_test' AND c.channel_id=?
        ORDER BY c.id,e.entry_id""",(guild_id,user_id,channel_id)).fetchall()
    sources,seen=[],set()
    for (root,) in roots:
        source=_meaning_source(conn,root,guild_id=guild_id,user_id=user_id,private_channel_id=channel_id)
        if source and source['row_id'] not in seen:
            sources.append(source)
            seen.add(source['row_id'])
    return tuple(sources)
'''


def scalar_owner():
    namespace = dict(vars(relationship))
    exec(compile(_SCALAR_OWNER, "<frozen-53d20e7-source-owner>", "exec"), namespace)
    return namespace["private_conversation_sources"]


class PrivateSourceReadEfficiencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scalar = staticmethod(scalar_owner())

    def setUp(self):
        self.fixture = lookup.PrivateConversationSourceLookupTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.conn = self.fixture.conn

    def sources(self, owner=relationship.private_conversation_sources):
        return owner(self.conn, guild_id=1, user_id=2, channel_id=99)

    def measure(self, owner):
        statements = []
        self.conn.set_trace_callback(lambda sql: statements.append(sql))
        tracemalloc.start()
        try:
            sources = self.sources(owner)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
            self.conn.set_trace_callback(None)
        return sources, len(statements), peak

    def test_real_scalar_cost_is_reduced_without_changing_any_source_dto(self):
        for row_id in range(1, 873):
            self.fixture.source(row_id, role="model" if row_id % 3 == 0 else "user")
            if row_id % 3 == 0:
                self.conn.execute("INSERT INTO conversation_response_participants VALUES(?,?,?)", (row_id,1,2))
        old, old_count, old_peak = self.measure(self.scalar)
        new, new_count, new_peak = self.measure(relationship.private_conversation_sources)
        self.assertEqual(new, old)
        self.assertEqual(relationship._meaning_digest(list(new)), relationship._meaning_digest(list(old)))
        self.assertEqual(len(new), 872)
        self.assertLess(new_count, old_count // 5, (old_count, new_count))
        # The existing returned complete sources dominate retained memory.
        # A bulk reader may add only a small transient window, not a second
        # retained history or a guild-wide source/audience materialization.
        self.assertLess(new_peak, old_peak + 1024 * 1024, (old_peak,new_peak))

    def test_first_valid_revision_and_all_negative_controls_match_scalar(self):
        root = self.fixture.source(1)
        self.fixture.clone_root(root,"aaa-invalid",lifecycle_status="superseded")
        self.fixture.clone_root(root,"bbb-valid")
        self.fixture.clone_root(root,"ccc-valid")
        model = self.fixture.source(2,role="model")
        self.conn.execute("INSERT INTO conversation_response_participants VALUES(?,?,?)", (2,1,2))
        self.fixture.source(3,user_id=3)
        self.fixture.source(4,guild_id=2)
        self.fixture.source(5,channel_id=100)
        self.fixture.source(6,policy="public_home")
        self.assertEqual(self.sources(), self.sources(self.scalar))
        self.assertEqual(self.sources()[0]["entry_id"],"bbb-valid")
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",("foreign",2,"retracts","bbb-valid","now"))
        self.assertEqual(self.sources(), self.sources(self.scalar))
        self.conn.execute("INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",("correction",1,"correction_of","bbb-valid","now"))
        self.assertEqual(self.sources(), self.sources(self.scalar))
        self.assertEqual(self.sources()[0]["entry_id"],"ccc-valid")
        self.conn.execute("INSERT INTO conversation_response_participants VALUES(?,?,?)", (2,1,3))
        self.assertEqual(self.sources(), self.sources(self.scalar))
        self.assertNotIn(model,[source["entry_id"] for source in self.sources()])

    def test_oversized_sources_are_streamed_and_unicode_and_nul_keep_scalar_semantics(self):
        for row_id in range(1, 33):
            self.fixture.source(row_id)
            self.conn.execute("UPDATE conversations SET content=? WHERE id=?", ("x" * 300000, row_id))
        for row_id, text in ((33,"Music \U0001f3b5 " * 400), (34,"intro\x00" + "z" * 5990)):
            root = self.fixture.source(row_id)
            self.conn.execute("UPDATE conversations SET content=? WHERE id=?", (text,row_id))
            self.conn.execute("UPDATE memory_ledger_entries SET normalized_value=? WHERE entry_id=?",
                              (text[:500],root))
        old, _, old_peak = self.measure(self.scalar)
        new, _, new_peak = self.measure(relationship.private_conversation_sources)
        self.assertEqual(new, old)
        self.assertEqual([source["row_id"] for source in new],[33,34])
        self.assertLess(new_peak,old_peak + 1024 * 1024,(old_peak,new_peak))

    def test_new_transactions_recheck_every_source_mutation(self):
        root = self.fixture.source(1)
        self.conn.commit()
        for mutation in ("edit","restore","correction","withdraw","privacy","restore_policy","delete"):
            self.conn.execute("BEGIN")
            self.assertEqual(self.sources(),self.sources(self.scalar))
            self.conn.rollback()
            if mutation == "edit":
                self.conn.execute("UPDATE conversations SET content='Changed source.' WHERE id=1")
            elif mutation == "restore":
                self.conn.execute("UPDATE conversations SET content='Test Member contribution 1.' WHERE id=1")
            elif mutation == "correction":
                self.conn.execute("INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",
                                  ("negative",1,"correction_of",root,"now"))
            elif mutation == "withdraw":
                self.conn.execute("DELETE FROM memory_ledger_lineage WHERE entry_id='negative'")
            elif mutation == "privacy":
                self.conn.execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=1")
            elif mutation == "restore_policy":
                self.conn.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE id=1")
            else:
                self.conn.execute("DELETE FROM conversations WHERE id=1")
            self.conn.commit()
            self.conn.execute("BEGIN")
            self.assertEqual(self.sources(),self.sources(self.scalar))
            self.assertEqual(relationship._meaning_digest(list(self.sources())),
                             relationship._meaning_digest(list(self.sources(self.scalar))))
            self.conn.rollback()
        self.assertEqual(self.sources(),())

    def test_overridden_owner_and_missing_lineage_preserve_scalar_errors(self):
        self.fixture.source(1)
        scalar_source = self.scalar.__globals__["_meaning_source"]
        with mock.patch.object(relationship,"_meaning_source",wraps=scalar_source) as owner:
            self.assertEqual(self.sources(),self.sources(self.scalar))
            self.assertEqual(owner.call_count,1)
        self.conn.execute("DROP TABLE memory_ledger_lineage")
        for owner in (self.scalar,relationship.private_conversation_sources):
            with self.assertRaisesRegex(sqlite3.OperationalError,"no such table"):
                self.sources(owner)

    def test_existing_primary_key_is_used_and_null_entry_ids_remain_ineligible(self):
        root = self.fixture.source(1)
        self.fixture.clone_root(root,None)
        indexed = self.conn.execute("PRAGMA index_info(sqlite_autoindex_memory_ledger_entries_1)").fetchall()
        self.assertEqual([row[2] for row in indexed],["entry_id"])
        self.assertEqual(self.sources(),self.sources(self.scalar))

    def test_rejected_original_is_not_hydrated_and_eligible_decode_errors_still_propagate(self):
        root = self.fixture.source(1)
        self.conn.execute("UPDATE conversations SET content=CAST(X'80' AS TEXT) WHERE id=1")
        for rejection in ("lifecycle", "privacy", "control"):
            self.conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='active',visibility='sealed_test' WHERE entry_id=?",(root,))
            self.conn.execute("DELETE FROM memory_ledger_lineage")
            if rejection == "lifecycle":
                self.conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='superseded' WHERE entry_id=?",(root,))
            elif rejection == "privacy":
                self.conn.execute("UPDATE memory_ledger_entries SET visibility='protected' WHERE entry_id=?",(root,))
            else:
                self.conn.execute("INSERT INTO memory_ledger_lineage VALUES(?,?,?,?,?)",("control",1,"retracts",root,"now"))
            with self.subTest(rejection=rejection):
                self.assertEqual(self.sources(self.scalar),())
                reads=[]
                def authorize(action,table,column,*unused):
                    if action==sqlite3.SQLITE_READ and table=="conversations" and column=="content":
                        reads.append(column)
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK
                self.conn.set_authorizer(authorize)
                try:
                    self.assertEqual(self.sources(),())
                finally:
                    # Python 3.9 cannot disable the authorizer with None.
                    self.conn.set_authorizer(lambda *_args: sqlite3.SQLITE_OK)
                self.assertEqual(reads,[])
        self.conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='active',visibility='sealed_test' WHERE entry_id=?",(root,))
        self.conn.execute("DELETE FROM memory_ledger_lineage")
        for owner in (self.scalar,relationship.private_conversation_sources):
            with self.assertRaises(sqlite3.OperationalError):
                self.sources(owner)

    def test_large_ledger_metadata_uses_scalar_validation_without_a_new_size_filter(self):
        root = self.fixture.source(1)
        large = "historical-field-" * 4000
        self.conn.execute("UPDATE memory_ledger_entries SET observed_at=? WHERE entry_id=?",(large,root))
        self.assertEqual(self.sources(),self.sources(self.scalar))
        self.assertEqual(self.sources()[0]["ledger"]["observed_at"],large)
        self.conn.execute("UPDATE memory_ledger_entries SET normalized_value=? WHERE entry_id=?",(large,root))
        self.assertEqual(self.sources(),self.sources(self.scalar))
        self.assertEqual(self.sources(),())
        self.conn.execute("UPDATE conversations SET content=CAST(X'80' AS TEXT) WHERE id=1")
        for owner in (self.scalar,relationship.private_conversation_sources):
            with self.assertRaises(sqlite3.OperationalError):
                self.sources(owner)


if __name__ == "__main__":
    unittest.main()
