import os, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bnl_memory_governance import *
from bnl_memory_ledger import ensure_memory_ledger_schema, subject_key_for_user


def make_conn():
    c=sqlite3.connect(':memory:')
    ensure_governance_schema(c)
    return c

def insert(c,guild=1,user=10,eid='e1',value='likes modular synths',source='first_party_record',vis='private',public=0,life='active',pred='preference'):
    now='2026-01-01T00:00:00+00:00'; subj=subject_key_for_user(user)
    c.execute("INSERT INTO memory_ledger_entries (entry_id,schema_version,guild_id,subject_key,subject_display_name,entry_type,predicate_key,normalized_value,source_class,source_table,source_row_id,source_role,visibility,confidence,public_usable,derived,projection,salience,observed_at,lifecycle_status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(eid,'memory_ledger_v1',guild,subj,'','claim',pred,value,source,'test',eid,'test',vis,'high',public,0,0,0.9,now,life,now,now))
    c.commit()

def req(**kw):
    d=dict(guild_id=1,subject_user_id=10,route_mode='normal',conversation_surface='test',channel_policy='public_home',visibility_allowance='private',user_text='remember synth',budget_chars=500,allowed_source_classes=('first_party_record','owner_correction','public_observation'))
    d.update(kw); return GovernanceRequest(**d)

def test_gates_default_off_and_live_requires_shadow(monkeypatch):
    monkeypatch.delenv(SHADOW_ENV, raising=False); monkeypatch.delenv(LIVE_ENV, raising=False)
    assert not shadow_enabled(); assert not live_enabled()
    monkeypatch.setenv(LIVE_ENV,'1')
    assert not live_enabled()
    monkeypatch.setenv(SHADOW_ENV,'1')
    assert live_enabled()

def test_guild_member_visibility_and_public_source_blind_exclusion():
    c=make_conn(); insert(c); insert(c,guild=2,eid='e2',value='wrong guild'); insert(c,user=11,eid='e3',value='wrong user')
    r=build_governed_context(c, req())
    assert 'modular synths' in r.rendered_context and 'wrong' not in r.rendered_context
    r2=build_governed_context(c, req(visibility_allowance='public_safe'))
    assert r2.rendered_context == ''
    insert(c,eid='e4',value='source blind',source='legacy_source_blind',vis='public',public=1)
    r3=build_governed_context(c, req(visibility_allowance='public_safe', allowed_source_classes=('legacy_source_blind',)))
    assert r3.rendered_context == ''

def test_correction_wins_and_forget_idempotent():
    c=make_conn(); insert(c,eid='base',value='old favorite color blue')
    ref=view_member_memory(c,guild_id=1,user_id=10)[0]['ref']
    res=correct_member_memory(c,guild_id=1,user_id=10,safe_ref=ref,corrected_text='favorite color is green')
    assert res['ok']
    out=build_governed_context(c, req(user_text='remember favorite color', allowed_source_classes=('first_party_record','owner_correction'))).rendered_context
    assert 'green' in out and 'blue' not in out
    assert forget_member_memory(c,guild_id=1,user_id=10,safe_ref=ref)['ok']
    assert forget_member_memory(c,guild_id=1,user_id=10,safe_ref=ref)['ok']

def test_complete_delete_rollback_and_other_member_untouched():
    c=make_conn(); insert(c); insert(c,user=11,eid='other',value='other data')
    c.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY, guild_id INTEGER, user_id INTEGER, content TEXT)"); c.execute("INSERT INTO conversations VALUES (1,1,10,'secret')")
    try:
        complete_delete_member_data(c,guild_id=1,user_id=10,confirmation='DELETE MY BNL DATA 1',inject_failure=True)
    except RuntimeError: pass
    assert c.execute("SELECT COUNT(*) FROM conversations WHERE user_id=10").fetchone()[0] == 1
    res=complete_delete_member_data(c,guild_id=1,user_id=10,confirmation='DELETE MY BNL DATA 1')
    assert res['ok']
    assert build_governed_context(c, req()).rendered_context == ''
    assert c.execute("SELECT normalized_value FROM memory_ledger_entries WHERE subject_key=?", (subject_key_for_user(11),)).fetchone()[0]=='other data'

def test_budget_dedupe_lifecycle_and_simple_greeting():
    c=make_conn(); insert(c,eid='a',value='same text same text'); insert(c,eid='b',value='same text same text'); insert(c,eid='x',value='expired',life='expired')
    r=build_governed_context(c, req(user_text='hi'))
    assert r.rendered_context == ''
    r=build_governed_context(c, req(user_text='remember same', budget_chars=40))
    assert len(r.selected) == 1
    assert 'expired' not in r.rendered_context
