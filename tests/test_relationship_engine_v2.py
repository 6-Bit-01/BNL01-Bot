import os, sqlite3, tempfile, unittest

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

from bnl_relationship_engine import *
from bnl_memory_ledger import ensure_memory_ledger_schema, shadow_conversation_row, subject_key_for_user
import bnl_moment_engine as moments
import bnl01_bot
from tests.unittest_compat import install_function_cases


def conn():
    c=sqlite3.connect(':memory:'); ensure_relationship_v2_schema(c); return c

def obs(c, text, rid, role='user', uid=2, guild=1, directed=True, policy='public_home', route='normal_chat', t='2026-01-01 00:00:00'):
    return observe_message(c,guild_id=guild,user_id=uid,role=role,content=text,source_row_id=rid,channel_policy=policy,route_mode=route,directed=directed,observed_at=t)

def state(c, uid=2, guild=1, t='2026-01-02T00:00:00+00:00'):
    return rebuild_state(c,guild_id=guild,subject_user_id=uid,evaluated_at=t)

def _case_flags_default_off_with_empty_mapping():
    assert not shadow_enabled({}) and not live_enabled({}) and not active_engagement_live_enabled({})

def _case_model_replies_zero_weight():
    c=conn(); obs(c,'friendly rival accepted',1,role='model'); s=state(c)
    assert all(s[d] == 0 for d in ('rapport','trust','familiarity','playfulness','support','mutuality'))
    assert s['rivalry_state'] != 'mutual_rivalry'

def _case_repeated_model_audits_create_no_ledger_projection_or_positive_state():
    c=conn(); ensure_memory_ledger_schema(c)
    for i in range(5): obs(c,'thanks friendly rival nemesis',i,role='model')
    s=state(c); assert all(s[d] == 0 for d in ('rapport','trust','familiarity','playfulness','support','mutuality'))
    assert c.execute("select count(*) from memory_ledger_entries where source_table='relationship_events_v2'").fetchone()[0] == 0

def _case_passive_unrelated_public_chatter_no_event_and_save_path(monkeypatch):
    tmp=tempfile.NamedTemporaryFile(delete=False); tmp.close(); monkeypatch.setattr(bnl01_bot,'DB_FILE',tmp.name); bnl01_bot.init_db(); monkeypatch.setenv(SHADOW_ENV,'1')
    bnl01_bot.save_user_message(2,'u',1,'thanks unrelated',channel_policy='public_home',route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,directed_to_bnl=False)
    with sqlite3.connect(tmp.name) as c:
        assert c.execute('select count(*) from relationship_events_v2').fetchone()[0] == 0
    c=conn(); assert obs(c,'talking to someone else thanks',1,directed=False,route='normal_chat') == ''



def _case_directed_sealed_test_save_path_creates_no_relationship_evidence(monkeypatch):
    tmp=tempfile.NamedTemporaryFile(delete=False); tmp.close(); monkeypatch.setattr(bnl01_bot,'DB_FILE',tmp.name); bnl01_bot.init_db(); monkeypatch.setenv(SHADOW_ENV,'1')
    bnl01_bot.save_user_message(2,'u',1,'thank you',channel_policy='sealed_test',route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,directed_to_bnl=True)
    with sqlite3.connect(tmp.name) as c:
        assert c.execute('select count(*) from relationship_events_v2').fetchone()[0] == 0
        assert c.execute("select rejection_reason from relationship_observation_diagnostics_v2").fetchone()[0] == 'sealed_test'

def _case_bot_save_paths_refresh_relationship_moment_links_in_shadow(monkeypatch):
    tmp=tempfile.NamedTemporaryFile(delete=False); tmp.close(); monkeypatch.setattr(bnl01_bot,'DB_FILE',tmp.name); bnl01_bot.init_db(); monkeypatch.setenv(SHADOW_ENV,'1')
    calls=[]
    def fake_refresh(conn, *, guild_id=None):
        calls.append(guild_id); return 0
    monkeypatch.setattr(bnl01_bot, 'refresh_relationship_v2_moment_links', fake_refresh)
    bnl01_bot.save_user_message(2,'u',1,'thank you',channel_policy='public_home',route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,directed_to_bnl=True)
    bnl01_bot.save_model_message(2,1,'audit only',channel_policy='public_home',route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT)
    assert calls == [1, 1]

def _case_direct_save_path_creates_private_review_only_projection(monkeypatch):
    tmp=tempfile.NamedTemporaryFile(delete=False); tmp.close(); monkeypatch.setattr(bnl01_bot,'DB_FILE',tmp.name); bnl01_bot.init_db(); monkeypatch.setenv(SHADOW_ENV,'1')
    bnl01_bot.save_user_message(2,'u',1,'thank you',channel_policy='public_home',route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,directed_to_bnl=True)
    with sqlite3.connect(tmp.name) as c:
        assert c.execute('select count(*) from relationship_events_v2').fetchone()[0] == 1
        row=c.execute("select visibility, public_usable, derived, projection, lifecycle_status from memory_ledger_entries where source_table='relationship_events_v2'").fetchone()
        assert row == ('private', 0, 1, 1, 'review_only')

def _case_shadow_would_select_without_live_and_does_not_consume_live_cooldown():
    c=conn(); obs(c,'thank you',1)
    r1=plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True,now='2026-01-02 00:00:00')
    r2=plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True,now='2026-01-02T01:00:00+00:00')
    assert r1['would_select'] and not r1['live_emission_allowed']
    assert r2['would_select'] and 'per_user_live_cooldown' not in r2['withheld_reason_codes']

def _case_live_cooldown_rolling_24h_actual_only_and_future_safe():
    c=conn(); obs(c,'thank you',1)
    c.execute("INSERT INTO relationship_engagement_shadow_runs (run_id,schema_version,guild_id,subject_user_id,subject_key,candidate_type,policy_eligible,would_select,live_emission_allowed,actual_emitted,withheld_reason_codes,route_mode,channel_policy,visibility_decision,cooldown_decision,simulated_cooldown_decision,source_class_counts_json,relevant_open_loop_count,relevant_moment_count,legacy_hash,v2_hash,processing_errors_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ('old',SCHEMA_VERSION,1,2,subject_key_for_user(2),'recognition',1,1,1,1,'[]','normal_chat','public_home','public','clear','clear','{}',0,0,'','', '[]','2026-01-01T00:00:00+00:00'))
    assert 'per_user_live_cooldown' not in plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True,now='2026-01-02T01:00:01+00:00')['withheld_reason_codes']
    c.execute("UPDATE relationship_engagement_shadow_runs SET created_at='2026-01-02T00:30:00+00:00' WHERE run_id='old'")
    assert 'per_user_live_cooldown' in plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True,now='2026-01-02T01:00:00+00:00')['withheld_reason_codes']
    c.execute("UPDATE relationship_engagement_shadow_runs SET created_at='2026-01-03T00:30:00+00:00' WHERE run_id='old'")
    assert 'per_user_live_cooldown' not in plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True,now='2026-01-02T01:00:00+00:00')['withheld_reason_codes']
    c.execute("UPDATE relationship_engagement_shadow_runs SET actual_emitted=0, created_at='2026-01-02T00:30:00+00:00' WHERE run_id='old'")
    assert 'per_user_live_cooldown' not in plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True,now='2026-01-02T01:00:00+00:00')['withheld_reason_codes']

def _case_false_rivalry_and_valid_mutual_rivalry_requires_model_acceptance_setting_and_context():
    c=conn()
    for i,txt in enumerate(['I opt in to friendly rival mode','lol friendly rival','haha nemesis'],1): obs(c,txt,i)
    assert state(c)['rivalry_state'] != 'mutual_rivalry'
    obs(c,'friendly rival accepted',4,role='model')
    assert state(c)['rivalry_state'] != 'mutual_rivalry'
    record_model_playful_rivalry_acceptance(c,guild_id=1,user_id=2,source_row_id='policy1')
    assert state(c)['rivalry_state'] == 'mutual_rivalry'
    set_member_setting(c,guild_id=1,user_id=2,playful_rivalry_enabled=False)
    assert state(c)['rivalry_state'] == 'neutral'
    set_member_setting(c,guild_id=1,user_id=2,playful_rivalry_enabled=True)
    assert state(c)['rivalry_state'] != 'mutual_rivalry'  # requires fresh explicit opt-in after disable

def _case_boundary_and_opt_out_override_engagement_and_proactive_reenable():
    c=conn(); obs(c,"don't follow up",1); assert state(c)['engagement_opt_out']
    assert 'member_opt_out' in plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True)['withheld_reason_codes']
    set_member_setting(c,guild_id=1,user_id=2,proactive_enabled=True)
    assert not state(c)['engagement_opt_out']
    obs(c,"don't joke rival stuff",2); obs(c,'I opt in to friendly rival mode',3); obs(c,'lol friendly rival',4); obs(c,'haha nemesis',5); record_model_playful_rivalry_acceptance(c,guild_id=1,user_id=2,source_row_id='policy2')
    assert state(c)['rivalry_state'] == 'neutral'

def _case_appreciation_collaboration_repair_decay_and_timestamps():
    c=conn(); obs(c,'thank you',1); assert state(c)['rapport'] > 0 and state(c)['trust'] == 0
    for i in range(2,6): obs(c,'let us work together',i,t='2026-01-01 00:00:00')
    s=state(c,t='2026-01-02T00:00:00+00:00'); assert s['trust'] > 0 and s['familiarity'] > 0
    obs(c,'bad bot',6,t='2026-01-01 00:00:00'); before=state(c,t='2026-01-02T00:00:00+00:00')['friction']; obs(c,'we are good',7,t='2026-01-02T02:00:00+00:00')
    after=state(c,t='2026-01-03 00:00:00')['friction']; assert after < before and state(c)['evidence_counts']['friction'] == 1

def _case_malformed_timestamp_uses_deterministic_fallback():
    c=conn(); obs(c,'thank you',1,t='not-a-date')
    assert state(c,t='2026-01-02T00:00:00+00:00') == state(c,t='2026-01-02T00:00:00+00:00')

def _case_same_member_guild_summary_isolation_and_live_gating(monkeypatch):
    c=conn(); obs(c,'thank you',1,uid=2,guild=1); state(c,uid=2,guild=1); monkeypatch.setenv(LIVE_ENV,'1')
    assert governed_summary(c,guild_id=1,user_id=2,target_user_id=3,route_mode='normal_chat',channel_policy='public_home',direct=True) == ''
    assert governed_summary(c,guild_id=2,user_id=2,target_user_id=2,route_mode='normal_chat',channel_policy='public_home',direct=True) == ''
    assert governed_summary(c,guild_id=1,user_id=2,target_user_id=2,route_mode='normal_chat',channel_policy='public_home',direct=True)
    assert governed_summary(c,guild_id=1,user_id=2,target_user_id=2,route_mode='relay',channel_policy='public_home',direct=True) == ''


def _case_context_builder_requires_live_direct_governance_and_no_scores(monkeypatch):
    tmp=tempfile.NamedTemporaryFile(delete=False); tmp.close(); monkeypatch.setattr(bnl01_bot,'DB_FILE',tmp.name); bnl01_bot.init_db(); monkeypatch.setenv(LIVE_ENV,'1')
    with sqlite3.connect(tmp.name) as c:
        obs(c,'thank you',1); state(c)
    passive = bnl01_bot.build_user_memory_context(2,1,route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,channel_policy='public_home',user_text='question',current_direct=False,governance_allowed=True)
    no_gov = bnl01_bot.build_user_memory_context(2,1,route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,channel_policy='public_home',user_text='question',current_direct=True,governance_allowed=False)
    live = bnl01_bot.build_user_memory_context(2,1,route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,channel_policy='public_home',user_text='question',current_direct=True,governance_allowed=True)
    assert 'Private relationship calibration' not in passive and 'Private relationship calibration' not in no_gov
    assert 'Private relationship calibration' in live and 'rapport=' not in live and '0.' not in live

def _case_settings_view_live_off_and_toggle_controls():
    c=conn(); assert 'proactive=enabled' in settings_summary(c,guild_id=1,user_id=2)
    set_member_setting(c,guild_id=1,user_id=2,proactive_enabled=False,playful_rivalry_enabled=False)
    assert 'proactive=disabled' in settings_summary(c,guild_id=1,user_id=2) and 'playful_rivalry=disabled' in settings_summary(c,guild_id=1,user_id=2)
    set_member_setting(c,guild_id=1,user_id=2,proactive_enabled=True,playful_rivalry_enabled=True)
    assert 'proactive=enabled' in settings_summary(c,guild_id=1,user_id=2) and 'playful_rivalry=enabled' in settings_summary(c,guild_id=1,user_id=2)

def _case_correction_forget_deactivates_underlying_event_and_delete_removes_projection():
    c=conn(); eid=obs(c,'thank you',1); assert state(c)['rapport'] > 0
    led=c.execute('select ledger_entry_id from relationship_event_ledger_links_v2 where event_id=?',(eid,)).fetchone()[0]
    propagate_ledger_lifecycle(c,guild_id=1,ledger_entry_id=led,lifecycle='forgotten')
    assert state(c)['rapport'] == 0
    counts=complete_delete_relationship_v2(c,guild_id=1,user_id=2)
    assert counts['relationship_events_v2'] == 1 and c.execute("select count(*) from memory_ledger_entries where source_table='relationship_events_v2'").fetchone()[0] == 0

def _case_moment_link_uses_conversation_ledger_lineage_and_guild_refresh_is_scoped(monkeypatch):
    monkeypatch.setenv('BNL_MEMORY_LEDGER_SHADOW_ENABLED','1'); monkeypatch.setenv('BNL_MOMENT_ENGINE_SHADOW_ENABLED','1')
    c=conn(); moments.ensure_moment_schema(c)
    base='2026-01-01T00:00:00+00:00'
    r1=shadow_conversation_row(c,row_id=1,user_id=2,user_name='U2',guild_id=1,role='user',content='synth patch needs drums',channel_policy='public_home',channel_id=10,channel_name='c10',route_mode='normal_chat',observed_at=base)
    moments.observe_ledger_entry(c,r1.entry_id)
    r2=shadow_conversation_row(c,row_id=2,user_id=3,user_name='U3',guild_id=1,role='user',content='synth patch drums lock',channel_policy='public_home',channel_id=10,channel_name='c10',route_mode='normal_chat',observed_at='2026-01-01T00:01:00+00:00')
    moments.observe_ledger_entry(c,r2.entry_id)
    r3=shadow_conversation_row(c,row_id=3,user_id=2,user_name='U2',guild_id=1,role='user',content='yes synth patch and drums lock together',channel_policy='public_home',channel_id=10,channel_name='c10',route_mode='normal_chat',observed_at='2026-01-01T00:02:00+00:00')
    moments.observe_ledger_entry(c,r3.entry_id); moments.sweep_expired_windows(c,guild_id=1,now='2026-01-01T00:10:00+00:00')
    eid=observe_message(c,guild_id=1,user_id=2,role='user',content='thank you',source_row_id=1,channel_policy='public_home',route_mode='normal_chat',directed=True,observed_at=base)
    assert refresh_moment_links(c,guild_id=1) >= 1
    assert c.execute("select count(*) from relationship_event_moment_links_v2 where event_id=? and lifecycle='active'", (eid,)).fetchone()[0] == 1
    c.execute("INSERT INTO relationship_event_moment_links_v2 VALUES ('other','m2',2,9,'active','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00')")
    refresh_moment_links(c,guild_id=1)
    assert c.execute("select lifecycle from relationship_event_moment_links_v2 where guild_id=2 and event_id='other'").fetchone()[0] == 'active'
    c.execute("UPDATE memory_moment_windows SET lifecycle_status='needs_review' WHERE guild_id=1")
    refresh_moment_links(c,guild_id=1)
    assert build_evaluation_report(c,guild_id=1)['moment_linked_events'] == 0

def _case_governed_open_loop_public_visibility_filter():
    c=conn(); obs(c,'thank you',1)
    c.execute("INSERT INTO memory_ledger_entries (entry_id,schema_version,guild_id,subject_key,entry_type,predicate_key,normalized_value,source_class,source_table,source_row_id,source_role,visibility,confidence,public_usable,derived,projection,salience,observed_at,lifecycle_status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ('ol_private','memory_ledger_v1',1,subject_key_for_user(2),'open_loop','open_loop','follow up','first_party_record','test','1','user','private','medium',0,0,0,0.5,'2026-01-01T00:00:00+00:00','active','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00'))
    r=plan_engagement(c,guild_id=1,user_id=2,candidate_type='open_loop_follow_up',route_mode='normal_chat',channel_policy='public_home',current_direct=True)
    assert r['relevant_open_loop_count'] == 0 and 'no_governed_open_loop' in r['withheld_reason_codes']
    c.execute("INSERT INTO memory_ledger_entries (entry_id,schema_version,guild_id,subject_key,entry_type,predicate_key,normalized_value,source_class,source_table,source_row_id,source_role,visibility,confidence,public_usable,derived,projection,salience,observed_at,lifecycle_status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ('ol_public','memory_ledger_v1',1,subject_key_for_user(2),'open_loop','open_loop','follow up','first_party_record','test','2','user','public_safe','medium',1,0,0,0.5,'2026-01-01T00:00:00+00:00','active','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00'))
    r=plan_engagement(c,guild_id=1,user_id=2,candidate_type='open_loop_follow_up',route_mode='normal_chat',channel_policy='public_home',current_direct=True)
    assert r['relevant_open_loop_count'] == 1

def _case_evaluation_report_truthful_and_shadow_no_discord_response():
    c=conn(); obs(c,'thank you',1); plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True)
    report=build_evaluation_report(c,guild_id=1)
    assert report['legacy_v2_comparison'] == 'not_collected' and report['policy_eligible_shadow_candidates'] == 1 and report['actual_live_emissions'] == 0


class RelationshipEngineV2Tests(unittest.TestCase):
    pass


install_function_cases(globals(), RelationshipEngineV2Tests)
