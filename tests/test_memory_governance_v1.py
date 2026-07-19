import os, sqlite3, sys, inspect, hashlib
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bnl_memory_governance import *
from bnl_memory_ledger import ensure_memory_ledger_schema, subject_key_for_user
from bnl_entity_intelligence import ensure_entity_intelligence_schema
from bnl_dossier_source_packets import subject_key as normalized_subject_key


def make_conn():
    c = sqlite3.connect(':memory:')
    ensure_governance_schema(c)
    return c


def insert(c, guild=1, user=10, eid='e1', value='likes modular synths', source='first_party_record', vis='private', public=0, life='active', pred='preference', etype='claim', observed='2026-01-01T00:00:00+00:00', derived=0, projection=0, source_table='test', source_revision=''):
    subj = subject_key_for_user(user)
    c.execute("INSERT INTO memory_ledger_entries (entry_id,schema_version,guild_id,subject_key,subject_display_name,entry_type,predicate_key,normalized_value,source_class,source_table,source_row_id,source_revision,source_role,visibility,confidence,public_usable,derived,projection,salience,observed_at,lifecycle_status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (eid, 'memory_ledger_v1', guild, subj, '', etype, pred, value, source, source_table, eid, source_revision, 'test', vis, 'high', public, derived, projection, 0.9, observed, life, observed, observed))
    c.commit()


def req(**kw):
    d = dict(guild_id=1, subject_user_id=10, route_mode='normal', conversation_surface='test', channel_policy='public_home', visibility_allowance='private', user_text='remember synths', budget_chars=500, allowed_source_classes=('first_party_record','owner_correction','public_observation','derived_summary','legacy_source_blind'), now='2026-07-16T00:00:00+00:00')
    d.update(kw)
    return GovernanceRequest(**d)


def test_gates_default_off_live_requires_both_and_legacy_unchanged(monkeypatch):
    monkeypatch.delenv(SHADOW_ENV, raising=False); monkeypatch.delenv(LIVE_ENV, raising=False)
    assert not shadow_enabled(); assert not live_enabled()
    monkeypatch.setenv(LIVE_ENV, '1')
    assert not live_enabled()
    monkeypatch.setenv(SHADOW_ENV, '1')
    assert live_enabled()


def test_unrelated_raw_conversation_observation_not_selected_but_durable_fact_is():
    c = make_conn()
    insert(c, eid='color', value='favorite color is green', pred='favorite_color', etype='preference')
    insert(c, eid='sandwich', value='I ate a sandwich and then watched television', pred='conversation', etype='observation')
    r = build_governed_context(c, req(user_text='what is my favorite color?'))
    assert 'favorite color is green' in r.rendered_context
    assert 'sandwich' not in r.rendered_context
    assert any(e.reason == 'non_durable_conversation' for e in r.exclusions)


def test_broad_recall_and_topic_specific_recall_differ_deterministically():
    c = make_conn()
    insert(c, eid='color', value='favorite color is green', pred='favorite_color')
    insert(c, eid='music', value='favorite music tool is a sampler', pred='preference')
    broad1 = build_governed_context(c, req(user_text='what do you remember about me?')).rendered_context
    broad2 = build_governed_context(c, req(user_text='what do you remember about me?')).rendered_context
    topic = build_governed_context(c, req(user_text='what is my favorite color?')).rendered_context
    assert broad1 == broad2
    assert 'sampler' in broad1 and 'green' in broad1
    assert 'green' in topic and 'sampler' not in topic


def test_guild_member_visibility_route_channel_and_current_message_isolation():
    c = make_conn()
    insert(c, eid='good', value='likes modular synths', pred='preference')
    insert(c, guild=2, eid='wrongguild', value='likes modular synths in wrong guild', pred='preference')
    insert(c, user=11, eid='wronguser', value='likes modular synths wrong user', pred='preference')
    r = build_governed_context(c, req())
    assert 'modular synths' in r.rendered_context and 'wrong' not in r.rendered_context
    public = build_governed_context(c, req(visibility_allowance='public_safe'))
    assert public.rendered_context == ''
    route = build_governed_context(c, req(allowed_source_classes=('owner_correction',)))
    assert route.rendered_context == ''
    current = build_governed_context(c, req(user_text='likes modular synths'))
    assert current.rendered_context == ''


def test_owner_correction_wins_and_forget_tombstone_idempotent_prevents_reintroduction():
    c = make_conn(); insert(c, eid='base', value='old favorite color blue', pred='favorite_color')
    ref = view_member_memory(c, guild_id=1, user_id=10)[0]['ref']
    res = correct_member_memory(c, guild_id=1, user_id=10, safe_ref=ref, corrected_text='favorite color is green')
    assert res['ok']
    out = build_governed_context(c, req(user_text='what is my favorite color?', allowed_source_classes=('first_party_record','owner_correction'))).rendered_context
    assert 'green' in out and 'blue' not in out
    assert forget_member_memory(c, guild_id=1, user_id=10, safe_ref=ref)['ok']
    assert forget_member_memory(c, guild_id=1, user_id=10, safe_ref=ref)['ok']
    # Later stale revision from same source is also suppressed.
    insert(c, eid='base_rev2', value='old favorite color blue', pred='favorite_color', source_table='test')
    c.execute("UPDATE memory_ledger_entries SET source_row_id='base', source_revision='rev2' WHERE entry_id='base_rev2'"); c.commit()
    assert 'blue' not in build_governed_context(c, req(user_text='favorite color')).rendered_context


def test_blocked_lifecycles_and_projection_loop_prevention():
    c = make_conn()
    for life in ('forgotten','retracted','superseded','expired','review_only','needs_review','quarantined','unresolved'):
        insert(c, eid=life, value=f'{life} synths', life=life, pred='preference')
    insert(c, eid='derived', value='derived synths loop', source='derived_summary', pred='preference', derived=1, projection=1)
    r = build_governed_context(c, req(user_text='remember synths'))
    assert r.rendered_context == ''
    assert r.diagnostics.excluded_by_reason['projection_lineage'] == 1
    assert r.diagnostics.excluded_by_reason['lifecycle'] == 8


def test_freshness_recency_per_source_caps_and_budget():
    c = make_conn()
    insert(c, eid='old', value='favorite snack is chips', pred='favorite_food', observed='2025-01-01T00:00:00+00:00')
    insert(c, eid='new', value='favorite snack is mango', pred='favorite_food', observed='2026-07-01T00:00:00+00:00')
    for i in range(6):
        insert(c, eid=f'goal{i}', value=f'open loop synth task {i}', pred='open_loop', etype='open_loop')
    r = build_governed_context(c, req(user_text='favorite snack'))
    assert 'mango' in r.rendered_context and 'chips' not in r.rendered_context
    capped = build_governed_context(c, req(user_text='what do you remember about me?', budget_chars=1000))
    assert capped.diagnostics.excluded_by_reason.get('per_source_cap', 0) >= 1
    tight = build_governed_context(c, req(user_text='what do you remember about me?', budget_chars=45))
    assert tight.diagnostics.token_budget_exclusions >= 1


def test_moment_engine_tables_detected_and_shadow_only():
    c = make_conn(); insert(c, eid='fact', value='likes synths', pred='preference')
    c.execute("CREATE TABLE memory_moment_windows (moment_id TEXT, guild_id INTEGER, lifecycle_status TEXT, canonical_ledger_entry_id TEXT, summary TEXT, updated_at TEXT)")
    c.execute("CREATE TABLE memory_moment_participants (moment_id TEXT, participant_key TEXT)")
    c.execute("INSERT INTO memory_moment_windows VALUES ('m1',1,'finalized','fact','PRIVATE MOMENT SUMMARY','')")
    c.execute("INSERT INTO memory_moment_participants VALUES ('m1',?)", (subject_key_for_user(10),))
    c.execute("INSERT INTO memory_moment_windows VALUES ('m2',1,'needs_review','','REVIEW','')")
    r = build_governed_context(c, req(user_text='remember synths'), include_review_moments=True)
    assert r.diagnostics.moment_candidate_count == 1
    assert r.diagnostics.moment_needs_review_excluded == 1
    assert 'PRIVATE MOMENT SUMMARY' not in r.rendered_context


def test_evaluation_report_counts_injected_violations():
    bad = MemoryCandidate('first_party_record','first_party_record','x','x',2,'discord_user:999','preference','claim','bad','unknown','high','needs_review',5,eligible_root=False,projection=True)
    diag = GovernanceDiagnostics(selected_by_source={'first_party_record':1}, excluded_by_reason={'budget':2}, token_budget_exclusions=2, duplicate_suppression=1, contradiction_resolutions=['a'], processing_errors=['boom'], legacy_vs_governed={'legacy_size':5})
    result = GovernanceResult('x', (bad,), (GovernanceExclusion('y','visibility'),), diag)
    report = build_evaluation_report([result], guild_id=1)
    assert report['cross_guild_violations'] >= 1
    assert report['review_only_or_needs_review_selected'] == 1
    assert report['budget_overruns'] == 2
    assert report['processing_errors'] == 1
    assert report['excluded_by_reason']['budget'] == 2
    assert report['rollback_readiness'] == 'fallback_required_before_live'


def test_processing_errors_mark_unsafe_for_live_fallback():
    r = build_governed_context(object(), req())  # type: ignore[arg-type]
    assert r.diagnostics.processing_errors


def test_view_authorization_redaction_and_correct_rejects_unauthorized():
    c = make_conn(); insert(c, eid='mine', value='my private synths', pred='preference'); insert(c, user=11, eid='other', value='other private secret', pred='preference')
    mine = view_member_memory(c, guild_id=1, user_id=10)
    assert len(mine) == 1 and 'other private secret' not in str(mine)
    assert not correct_member_memory(c, guild_id=1, user_id=10, safe_ref='mem_notreal', corrected_text='x')['ok']


def test_complete_delete_real_schemas_shared_moment_repeat_receipt_and_rollback():
    c = make_conn(); insert(c, eid='mine', value='delete me', pred='preference'); insert(c, user=11, eid='other', value='keep other', pred='preference')
    c.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY, guild_id INTEGER, user_id INTEGER, content TEXT)"); c.execute("INSERT INTO conversations VALUES (1,1,10,'secret')")
    c.execute("CREATE TABLE bnl_journal_source_events (event_seq INTEGER PRIMARY KEY, guild_id INTEGER NOT NULL, source_kind TEXT NOT NULL, subject_ref TEXT NOT NULL)")
    c.execute("CREATE TRIGGER trg_bnl_journal_sources_no_delete BEFORE DELETE ON bnl_journal_source_events BEGIN SELECT RAISE(ABORT, 'bnl_journal_source_events_immutable'); END")
    c.executemany(
        "INSERT INTO bnl_journal_source_events VALUES (?,?,?,?)",
        [
            (1, 1, 'discord_message', 'discord_user:10'),
            (2, 1, 'discord_message', 'discord_user:11'),
            (3, 2, 'discord_message', 'discord_user:10'),
            (4, 1, 'website_relay', 'discord_user:10'),
        ],
    )
    c.execute("CREATE TABLE response_style_log (id INTEGER, guild_id INTEGER, user_id INTEGER, style_key TEXT)"); c.execute("INSERT INTO response_style_log VALUES (1,1,10,'x')")
    c.execute("CREATE TABLE memory_moment_windows (moment_id TEXT, guild_id INTEGER, lifecycle_status TEXT, summary TEXT, updated_at TEXT)")
    c.execute("CREATE TABLE memory_moment_members (moment_id TEXT, ledger_entry_id TEXT)")
    c.execute("CREATE TABLE memory_moment_participants (moment_id TEXT, participant_key TEXT)")
    c.execute("INSERT INTO memory_moment_windows VALUES ('m1',1,'finalized','secret summary','')")
    c.execute("INSERT INTO memory_moment_members VALUES ('m1','mine')")
    c.execute("INSERT INTO memory_moment_participants VALUES ('m1',?)", (subject_key_for_user(10),))
    c.execute("INSERT INTO memory_moment_participants VALUES ('m1',?)", (subject_key_for_user(11),))
    c.commit()
    try:
        complete_delete_member_data(c, guild_id=1, user_id=10, confirmation='DELETE MY BNL DATA 1', inject_failure=True)
    except RuntimeError:
        pass
    assert c.execute("SELECT COUNT(*) FROM conversations WHERE user_id=10").fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM bnl_journal_source_events WHERE event_seq=1").fetchone()[0] == 1
    res = complete_delete_member_data(c, guild_id=1, user_id=10, confirmation='DELETE MY BNL DATA 1')
    assert res['ok']
    assert res['row_counts']['bnl_journal_source_events'] == 1
    assert c.execute("SELECT COUNT(*) FROM conversations WHERE user_id=10").fetchone()[0] == 0
    assert {row[0] for row in c.execute("SELECT event_seq FROM bnl_journal_source_events")} == {2, 3, 4}
    assert c.execute("SELECT COUNT(*) FROM memory_moment_participants WHERE participant_key=?", (subject_key_for_user(10),)).fetchone()[0] == 0
    assert c.execute("SELECT COUNT(*) FROM memory_moment_participants WHERE participant_key=?", (subject_key_for_user(11),)).fetchone()[0] == 1
    assert c.execute("SELECT summary,lifecycle_status FROM memory_moment_windows WHERE moment_id='m1'").fetchone() == ('', 'needs_review')
    receipt_row = c.execute("SELECT subject_hash,row_counts_json FROM memory_governance_receipts WHERE action='complete_delete'").fetchone()
    assert str(10) not in receipt_row[0] and 'secret' not in receipt_row[1]
    import json; assert json.loads(receipt_row[1])['bnl_journal_source_events'] == 1
    try:
        c.execute("DELETE FROM bnl_journal_source_events WHERE event_seq=2")
        assert False, 'archive delete guard was not restored'
    except sqlite3.IntegrityError:
        c.rollback()
    c.execute("INSERT INTO conversations VALUES (2,1,10,'new secret')"); c.commit()
    assert complete_delete_member_data(c, guild_id=1, user_id=10, confirmation='DELETE MY BNL DATA 1')['ok']
    assert c.execute("SELECT COUNT(*) FROM conversations WHERE user_id=10").fetchone()[0] == 0


def test_complete_delete_lays_journal_privacy_groundwork_without_touching_published_prose():
    import json

    c = make_conn()
    c.executescript("""
        CREATE TABLE bnl_journal_source_events (
            event_seq INTEGER PRIMARY KEY, guild_id INTEGER NOT NULL,
            source_kind TEXT NOT NULL, subject_ref TEXT NOT NULL
        );
        CREATE TRIGGER trg_bnl_journal_sources_no_delete
        BEFORE DELETE ON bnl_journal_source_events
        BEGIN SELECT RAISE(ABORT, 'bnl_journal_source_events_immutable'); END;
        CREATE TABLE bnl_journal_entries (
            entry_id TEXT, revision INTEGER, guild_id INTEGER, lifecycle_state TEXT,
            PRIMARY KEY(entry_id, revision)
        );
        CREATE TABLE bnl_journal_private_metadata (
            entry_id TEXT, revision INTEGER, guild_id INTEGER, metadata_json TEXT,
            lifecycle_state TEXT, updated_at TEXT, PRIMARY KEY(entry_id, revision)
        );
        CREATE TABLE bnl_journal_observations (
            observation_id TEXT PRIMARY KEY, guild_id INTEGER,
            aggregate_counts_json TEXT, topic_counts_json TEXT, subject_counts_json TEXT,
            representative_sources_json TEXT, lifecycle_state TEXT,
            journal_entry_id TEXT, journal_revision INTEGER, updated_at TEXT
        );
    """)
    target = 'discord_user:10'
    # The prefix-overlapping ID proves deletion uses exact identity matching.
    other = 'discord_user:100'
    c.executemany(
        "INSERT INTO bnl_journal_source_events VALUES (?,?,?,?)",
        [(1, 1, 'discord_message', target), (2, 1, 'discord_message', other)],
    )
    c.executemany(
        "INSERT INTO bnl_journal_entries VALUES (?,?,?,?)",
        [('published-entry', 1, 1, 'published'), ('target-draft', 1, 1, 'draft'), ('other-draft', 1, 1, 'draft')],
    )

    def metadata(subject, ref_id, summary):
        return json.dumps({
            'subjectRefs': [subject],
            'supportingConversationRefs': [{'refId': ref_id, 'subjectRef': subject}],
            'sourceSummaries': [{'refId': ref_id, 'summary': summary}],
            'sourceRefIds': {'A': [ref_id]},
            'topicTags': [summary],
            'continuityNotes': [summary],
        })

    c.executemany(
        "INSERT INTO bnl_journal_private_metadata VALUES (?,?,?,?,?,?)",
        [
            ('published-entry', 1, 1, metadata(target, 'fresh:1', 'secretword'), 'published', ''),
            ('target-draft', 1, 1, metadata(target, 'fresh:1', 'secretword'), 'draft', ''),
            ('other-draft', 1, 1, metadata(other, 'fresh:2', 'synthwave'), 'draft', ''),
        ],
    )
    representatives = [
        {'refId': 'fresh:1', 'sourceKind': 'conversation', 'subjectRef': target, 'summary': 'secretword'},
        {'refId': 'fresh:2', 'sourceKind': 'conversation', 'subjectRef': other, 'summary': 'synthwave'},
    ]
    c.execute(
        "INSERT INTO bnl_journal_observations VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            'obs-1', 1,
            json.dumps({'eligibleConversations': 2, 'participants': 2, 'archivedSourceEvents': 2}),
            json.dumps({'secretword': 1, 'synthwave': 1}),
            json.dumps({target: 1, other: 1}), json.dumps(representatives),
            'observed', 'target-draft', 1, '',
        ),
    )
    c.commit()

    try:
        complete_delete_member_data(
            c, guild_id=1, user_id=10,
            confirmation='DELETE MY BNL DATA 1', inject_failure=True,
        )
    except RuntimeError:
        pass
    assert c.execute("SELECT COUNT(*) FROM bnl_journal_entries WHERE entry_id='target-draft'").fetchone()[0] == 1
    assert target in c.execute("SELECT representative_sources_json FROM bnl_journal_observations").fetchone()[0]

    result = complete_delete_member_data(c, guild_id=1, user_id=10, confirmation='DELETE MY BNL DATA 1')
    assert result['ok']
    assert c.execute("SELECT COUNT(*) FROM bnl_journal_entries WHERE entry_id='published-entry'").fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM bnl_journal_entries WHERE entry_id='target-draft'").fetchone()[0] == 0
    assert c.execute("SELECT COUNT(*) FROM bnl_journal_entries WHERE entry_id='other-draft'").fetchone()[0] == 1
    published_meta = c.execute("SELECT metadata_json FROM bnl_journal_private_metadata WHERE entry_id='published-entry'").fetchone()[0]
    assert target not in published_meta and 'secretword' not in published_meta
    observation = c.execute(
        "SELECT aggregate_counts_json,topic_counts_json,subject_counts_json,representative_sources_json,journal_entry_id "
        "FROM bnl_journal_observations WHERE observation_id='obs-1'"
    ).fetchone()
    assert json.loads(observation[0])['eligibleConversations'] == 1
    assert json.loads(observation[1]) == {'synthwave': 1}
    assert json.loads(observation[2]) == {other: 1}
    assert all(item.get('subjectRef') != target for item in json.loads(observation[3]))
    assert any(item.get('subjectRef') == other for item in json.loads(observation[3]))
    assert observation[4] is None


def test_clearhistory_wording_and_queue_subsystem_untouched_static():
    source = Path('bnl01_bot.py').read_text()
    assert 'conversation rows' in source
    assert 'Durable facts, profile data, relationship data, and derived memory were not removed' in source
    governance_source = Path('bnl_memory_governance.py').read_text()
    assert 'queue_public_snapshot' not in governance_source
    assert 'payments' not in governance_source and 'playback' not in governance_source and 'availability' not in governance_source

def test_forget_original_removes_active_correction_chain_and_obsolete_correction_rejected():
    c = make_conn(); insert(c, eid='blue', value='favorite color is blue', pred='favorite_color')
    original_ref = view_member_memory(c, guild_id=1, user_id=10)[0]['ref']
    assert correct_member_memory(c, guild_id=1, user_id=10, safe_ref=original_ref, corrected_text='favorite color is green')['ok']
    # Correcting the obsolete original is rejected, but forgetting through its old ref resolves the chain.
    assert not correct_member_memory(c, guild_id=1, user_id=10, safe_ref=original_ref, corrected_text='favorite color is red')['ok']
    assert forget_member_memory(c, guild_id=1, user_id=10, safe_ref=original_ref)['ok']
    rendered = build_governed_context(c, req(user_text='what is my favorite color?', allowed_source_classes=('first_party_record','owner_correction'))).rendered_context
    assert 'blue' not in rendered and 'green' not in rendered


def test_complete_delete_actual_member_entity_broadcast_and_shadow_tables():
    c = make_conn(); insert(c, eid='mine', value='delete me', pred='preference')
    ensure_entity_intelligence_schema(c)
    deleted_key = normalized_subject_key('Deleted Name')
    preferred_key = normalized_subject_key('Preferred Deleted')
    c.execute("CREATE TABLE user_profiles (guild_id INTEGER, user_id INTEGER, display_name TEXT, preferred_name TEXT)")
    c.execute("INSERT INTO user_profiles VALUES (1,10,'Deleted Name','Preferred Deleted')")
    c.execute("CREATE TABLE community_presence (guild_id INTEGER, member_user_id INTEGER, subject_key TEXT, member_key TEXT, note TEXT)")
    c.execute("INSERT INTO community_presence VALUES (1,NULL,?,'other-key','mine subject')", (deleted_key,))
    c.execute("INSERT INTO community_presence VALUES (1,NULL,'other-key',?,'mine member')", (preferred_key,))
    c.execute("INSERT INTO community_presence VALUES (1,11,'other-member','other-member','same guild other')")
    c.execute("INSERT INTO community_presence VALUES (2,NULL,?,'other-key','cross guild stays')", (deleted_key,))
    c.execute("CREATE TABLE member_activity_events (guild_id INTEGER, member_user_id TEXT, note TEXT)"); c.execute("INSERT INTO member_activity_events VALUES (1,'10','mine')")
    c.execute("CREATE TABLE entity_evidence_events (guild_id INTEGER, matched_user_id TEXT, subject_key TEXT, entity_key TEXT, note TEXT)")
    c.execute("INSERT INTO entity_evidence_events VALUES (1,'10','x','x','mine matched id')")
    c.execute("INSERT INTO entity_evidence_events VALUES (1,NULL,?,?,'mine name key')", (deleted_key, preferred_key))
    c.execute("INSERT INTO entity_evidence_events VALUES (2,NULL,?,?,'cross guild stays')", (deleted_key, preferred_key))
    c.execute("INSERT INTO entity_intelligence_facts (guild_id,subject_key,subject_name,fact_type,fact_label,fact_value,visibility,authority) VALUES (1,?,'Deleted Name','profile','color','green','review_only','observed')", (deleted_key,))
    c.execute("INSERT INTO entity_intelligence_facts (guild_id,subject_key,subject_name,fact_type,fact_label,fact_value,visibility,authority) VALUES (1,'unrelated','Other','profile','color','blue','review_only','observed')")
    c.execute("INSERT INTO entity_intelligence_facts (guild_id,subject_key,subject_name,fact_type,fact_label,fact_value,visibility,authority) VALUES (2,?,'Deleted Name','profile','color','green','review_only','observed')", (deleted_key,))
    c.execute("INSERT INTO entity_intelligence_edges (guild_id,subject_key,object_key,object_label,relation_type,visibility,authority) VALUES (1,?,'project-a','Project A','likes','review_only','observed')", (deleted_key,))
    c.execute("INSERT INTO entity_intelligence_edges (guild_id,subject_key,object_key,object_label,relation_type,visibility,authority) VALUES (1,'project-b',?,'Deleted Name','mentions','review_only','observed')", (preferred_key,))
    c.execute("INSERT INTO entity_intelligence_edges (guild_id,subject_key,object_key,object_label,relation_type,visibility,authority) VALUES (1,'unrelated','project-c','Project C','likes','review_only','observed')")
    c.execute("INSERT INTO entity_intelligence_edges (guild_id,subject_key,object_key,object_label,relation_type,visibility,authority) VALUES (2,?,'project-a','Project A','likes','review_only','observed')", (deleted_key,))
    c.execute("INSERT INTO entity_activity_rollups (guild_id,subject_key,activity_type) VALUES (1,?,'chat')", (deleted_key,))
    c.execute("INSERT INTO entity_open_questions (guild_id,subject_key,question) VALUES (1,?,'ask')", (deleted_key,))
    c.execute("INSERT INTO entity_profile_snapshots (guild_id,subject_key,summary_json) VALUES (1,?,?)", (deleted_key, '{}'))
    c.execute("INSERT INTO entity_scouting_queue (guild_id,subject_key,reason,source) VALUES (1,?,'r','s')", (deleted_key,))
    c.execute("CREATE TABLE broadcast_memory (guild_id INTEGER, submitted_by_user_id TEXT, submitted_by_name TEXT, corrected_by_user_id TEXT, corrected_by_name TEXT, show_note TEXT)")
    c.execute("INSERT INTO broadcast_memory VALUES (1,'10','Deleted Name','10','Deleted Name','show content stays')")
    c.execute("INSERT INTO memory_governance_shadow_runs VALUES ('r1',1,?,?,?,?,?,?,?,?,?,?,?)", (hashlib.sha256(subject_key_for_user(10).encode()).hexdigest()[:16],'route','policy','now','h',1,'lh',2,1,'{}','[]'))
    c.commit()
    res = complete_delete_member_data(c, guild_id=1, user_id=10, confirmation='DELETE MY BNL DATA 1')
    assert res['ok']
    assert c.execute("SELECT COUNT(*) FROM member_activity_events").fetchone()[0] == 0
    assert c.execute("SELECT COUNT(*) FROM entity_evidence_events WHERE guild_id=1").fetchone()[0] == 0
    assert c.execute("SELECT COUNT(*) FROM entity_evidence_events WHERE guild_id=2").fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM community_presence WHERE guild_id=1 AND (member_user_id=10 OR subject_key=? OR member_key=?)", (deleted_key, preferred_key)).fetchone()[0] == 0
    assert c.execute("SELECT COUNT(*) FROM community_presence WHERE member_user_id=11").fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM community_presence WHERE guild_id=2 AND subject_key=?", (deleted_key,)).fetchone()[0] == 1
    for table in ('entity_intelligence_facts','entity_activity_rollups','entity_open_questions','entity_profile_snapshots','entity_scouting_queue'):
        assert c.execute(f"SELECT COUNT(*) FROM {table} WHERE guild_id=1 AND subject_key=?", (deleted_key,)).fetchone()[0] == 0
    assert c.execute("SELECT COUNT(*) FROM entity_intelligence_facts WHERE guild_id=1 AND subject_key='unrelated'").fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM entity_intelligence_facts WHERE guild_id=2 AND subject_key=?", (deleted_key,)).fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM entity_intelligence_edges WHERE guild_id=1 AND (subject_key IN (?,?) OR object_key IN (?,?))", (deleted_key, preferred_key, deleted_key, preferred_key)).fetchone()[0] == 0
    assert c.execute("SELECT COUNT(*) FROM entity_intelligence_edges WHERE guild_id=1 AND subject_key='unrelated'").fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM entity_intelligence_edges WHERE guild_id=2 AND subject_key=?", (deleted_key,)).fetchone()[0] == 1
    assert c.execute("SELECT submitted_by_user_id,submitted_by_name,corrected_by_user_id,corrected_by_name,show_note FROM broadcast_memory").fetchone() == (None, None, None, None, 'show content stays')
    assert c.execute("SELECT COUNT(*) FROM memory_governance_shadow_runs").fetchone()[0] == 0
    assert complete_delete_member_data(c, guild_id=1, user_id=10, confirmation='DELETE MY BNL DATA 1')['ok']


def test_complete_delete_moments_are_guild_scoped_and_canonical_payload_scrubbed():
    c = make_conn(); insert(c, eid='mine', value='secret summary from DeletedName', pred='preference')
    # capture display name before deletion
    c.execute("CREATE TABLE user_profiles (guild_id INTEGER, user_id INTEGER, display_name TEXT, preferred_name TEXT)"); c.execute("INSERT INTO user_profiles VALUES (1,10,'DeletedName','PreferredDeleted')")
    c.execute("CREATE TABLE memory_moment_windows (moment_id TEXT, guild_id INTEGER, lifecycle_status TEXT, canonical_ledger_entry_id TEXT, summary TEXT, updated_at TEXT)")
    c.execute("CREATE TABLE memory_moment_members (moment_id TEXT, ledger_entry_id TEXT)")
    c.execute("CREATE TABLE memory_moment_participants (moment_id TEXT, participant_key TEXT)")
    canonical_payload = '{"participants":["discord_user:10","discord_user:11"],"summary":"secret summary from DeletedName"}'
    insert(c, guild=1, user=99, eid='canon1', value=canonical_payload, source='derived_summary', pred='preference', derived=1, projection=1)
    c.execute("UPDATE memory_ledger_entries SET subject_key='moment:m1', source_table='memory_moment_windows', source_row_id='m1' WHERE entry_id='canon1'")
    c.execute("INSERT INTO memory_ledger_participants VALUES ('canon1',1,?,'DeletedName','participant',0,'now')", (subject_key_for_user(10),))
    c.execute("INSERT INTO memory_ledger_participants VALUES ('canon1',1,?,'Other','participant',1,'now')", (subject_key_for_user(11),))
    c.execute("INSERT INTO memory_moment_windows VALUES ('m1',1,'finalized','canon1','secret summary from DeletedName','')")
    c.execute("INSERT INTO memory_moment_windows VALUES ('m2',2,'finalized','','guild2 untouched','old')")
    c.execute("INSERT INTO memory_moment_members VALUES ('m1','mine')")
    c.execute("INSERT INTO memory_moment_participants VALUES ('m1',?)", (subject_key_for_user(10),))
    c.execute("INSERT INTO memory_moment_participants VALUES ('m1',?)", (subject_key_for_user(11),))
    c.execute("INSERT INTO memory_moment_participants VALUES ('m2',?)", (subject_key_for_user(10),))
    before_g2 = c.execute("SELECT * FROM memory_moment_participants WHERE moment_id='m2'").fetchall()
    c.commit()
    assert complete_delete_member_data(c, guild_id=1, user_id=10, confirmation='DELETE MY BNL DATA 1')['ok']
    assert c.execute("SELECT COUNT(*) FROM memory_moment_participants WHERE moment_id='m1' AND participant_key=?", (subject_key_for_user(10),)).fetchone()[0] == 0
    assert c.execute("SELECT * FROM memory_moment_participants WHERE moment_id='m2'").fetchall() == before_g2
    payload, life = c.execute("SELECT normalized_value,lifecycle_status FROM memory_ledger_entries WHERE entry_id='canon1'").fetchone()
    hay = '\n'.join(str(x) for x in c.execute("SELECT * FROM memory_ledger_entries UNION ALL SELECT * FROM memory_ledger_entries").fetchall())
    assert 'discord_user:10' not in payload and 'DeletedName' not in payload and 'secret summary' not in payload
    assert life == 'needs_review'
    assert c.execute("SELECT COUNT(*) FROM memory_ledger_participants WHERE entry_id='canon1' AND participant_key=?", (subject_key_for_user(10),)).fetchone()[0] == 0
    assert c.execute("SELECT COUNT(*) FROM memory_ledger_lineage l WHERE l.guild_id=1 AND (l.entry_id NOT IN (SELECT entry_id FROM memory_ledger_entries WHERE guild_id=1) OR l.target_entry_id NOT IN (SELECT entry_id FROM memory_ledger_entries WHERE guild_id=1))").fetchone()[0] == 0


def test_route_policy_violations_and_shadow_retention_are_real():
    c = make_conn(); insert(c, eid='sealed', value='sealed synths', pred='preference')
    c.execute("UPDATE memory_ledger_entries SET channel_policy='sealed_test' WHERE entry_id='sealed'"); c.commit()
    r = build_governed_context(c, req(user_text='remember synths', channel_policy='public_home'))
    assert r.rendered_context == '' and 'invalid_route_channel_policy' in r.diagnostics.excluded_by_reason
    report = build_evaluation_report([r], guild_id=1)
    assert report['invalid_route_channel_policy_selections'] >= 1
    for i in range(505):
        persist_shadow_diagnostics(c, req(), r, 'legacy')
    assert c.execute("SELECT COUNT(*) FROM memory_governance_shadow_runs WHERE guild_id=1").fetchone()[0] <= 500
