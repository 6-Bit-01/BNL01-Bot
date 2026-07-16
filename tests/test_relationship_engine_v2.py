import os, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bnl_relationship_engine import *

def conn():
    c=sqlite3.connect(':memory:'); ensure_relationship_v2_schema(c); return c

def obs(c, text, rid, role='user', uid=2, guild=1, directed=True, policy='public_home', route='normal_chat', t='2026-01-01T00:00:00+00:00'):
    return observe_message(c,guild_id=guild,user_id=uid,role=role,content=text,source_row_id=rid,channel_policy=policy,route_mode=route,directed=directed,observed_at=t)

def state(c, uid=2, guild=1, t='2026-01-02T00:00:00+00:00'):
    return rebuild_state(c,guild_id=guild,subject_user_id=uid,evaluated_at=t)

def test_model_replies_zero_weight_and_shadow_flags_off_by_default():
    c=conn(); obs(c,'try these steps',1,role='model'); s=state(c)
    assert all(s[d] == 0 for d in ('rapport','trust','familiarity','playfulness','support','mutuality'))
    assert not shadow_enabled({}) and not live_enabled({}) and not active_engagement_live_enabled({})

def test_passive_unrelated_public_chatter_no_event():
    c=conn(); assert obs(c,'talking to someone else thanks',1,directed=False,route='passive_capture') == ''
    assert c.execute('select count(*) from relationship_events_v2').fetchone()[0] == 0

def test_single_negative_or_joke_does_not_create_rivalry():
    for txt in ['bad bot','lol jk','i disagree','that is wrong']:
        c=conn(); obs(c,txt,1); assert state(c)['rivalry_state'] != 'mutual_rivalry'

def test_explicit_mutual_playful_rivalry_requires_opt_in_and_context():
    c=conn(); obs(c,'I opt in to friendly rival mode',1); obs(c,'lol rival mode',2); obs(c,'haha nemesis',3)
    assert state(c)['rivalry_state'] == 'mutual_rivalry'

def test_boundary_or_opt_out_blocks_rivalry_and_engagement():
    c=conn(); obs(c,"don't follow up",1); obs(c,'I opt in to friendly rival mode',2); obs(c,'lol rival mode',3); obs(c,'haha nemesis',4)
    assert state(c)['rivalry_state'] == 'neutral'
    r=plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True,now='2026-01-03T00:00:00+00:00')
    assert 'member_opt_out' in r['withheld_reason_codes'] and not r['selected']

def test_appreciation_improves_rapport_not_trust():
    c=conn(); obs(c,'thank you',1); s=state(c); assert s['rapport'] > 0 and s['trust'] == 0

def test_constructive_collaboration_gradually_increases_trust_and_familiarity():
    c=conn(); [obs(c,'let us work together on this',i) for i in range(1,5)]; s=state(c)
    assert 0 < s['trust'] < 1 and 0 < s['familiarity'] < 1

def test_distinct_event_types_and_repair_reduces_friction_without_erasing_history():
    c=conn(); texts=['help me','that is wrong','bad bot','my bad','let us fix this','we are good']
    for i,t in enumerate(texts,1): obs(c,t,i)
    types={r[0] for r in c.execute('select event_type from relationship_events_v2')}
    assert {'support_request','correction','friction','apology','repair_attempt','repair_accepted'} <= types
    s=state(c); assert s['friction'] < .12 and s['evidence_counts']['friction'] == 1

def test_decay_deterministic_fixed_clock():
    c=conn(); obs(c,'bad bot',1,t='2026-01-01T00:00:00+00:00')
    assert state(c,t='2026-02-01T00:00:00+00:00') == state(c,t='2026-02-01T00:00:00+00:00')
    assert state(c,t='2026-03-01T00:00:00+00:00')['friction'] < state(c,t='2026-01-02T00:00:00+00:00')['friction']

def test_lifecycle_exclusions_cross_guild_member_and_public_summary_safety(monkeypatch):
    c=conn(); obs(c,'thanks',1,uid=2,guild=1); obs(c,'thanks',2,uid=3,guild=1); obs(c,'thanks',3,uid=2,guild=9)
    c.execute("update relationship_events_v2 set lifecycle='forgotten' where guild_id=1 and subject_user_id=2")
    assert state(c,uid=2,guild=1)['rapport'] == 0
    monkeypatch.setenv(LIVE_ENV,'1')
    assert 'trust' not in governed_summary(c,guild_id=1,user_id=3,route_mode='normal_chat',channel_policy='public_home')
    assert governed_summary(c,guild_id=9,user_id=2,route_mode='passive_capture',channel_policy='public_home') == ''

def test_member_settings_complete_delete_and_no_mentions_dormant_echo():
    c=conn(); obs(c,"don't follow up",1); set_member_setting(c,guild_id=1,user_id=2,proactive_enabled=False,playful_rivalry_enabled=False)
    for ct in ['recognition','open_loop_follow_up','curiosity_question','dormant_signal_echo']:
        r=plan_engagement(c,guild_id=1,user_id=2,candidate_type=ct,route_mode='normal_chat',channel_policy='public_home',current_direct=False,now=f'2026-01-0{2+len(ct)%5}T00:00:00+00:00')
        assert not r['selected'] and '@' not in r.get('candidate_text','') and ('current' not in r.get('candidate_text','').lower())
    counts=complete_delete_relationship_v2(c,guild_id=1,user_id=2)
    assert counts['relationship_events_v2'] and c.execute('select count(*) from relationship_events_v2 where guild_id=1 and subject_user_id=2').fetchone()[0] == 0

def test_cooldown_shadow_no_user_output_and_report():
    c=conn(); obs(c,'thanks',1)
    r1=plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True,now='2026-01-02T00:00:00+00:00')
    r2=plan_engagement(c,guild_id=1,user_id=2,candidate_type='recognition',route_mode='normal_chat',channel_policy='public_home',current_direct=True,now='2026-01-02T01:00:00+00:00')
    assert not r1['selected'] and 'per_user_cooldown' in r2['withheld_reason_codes']
    report=build_evaluation_report(c,guild_id=1); assert report['eligible_user_authored_events'] == 1 and report['rollback_readiness']['relationship_live_default_off']
