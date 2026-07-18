from __future__ import annotations

import hashlib, json, re, sqlite3, urllib.error, urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Optional

PUBLIC_POLICIES = {"public_home", "public_context", "public_selective"}
FORBIDDEN_SOURCE_TABLES = {"relationship_journal", "relationship_state", "user_profiles", "user_memory_facts", "user_habits", "memory_tiers", "queue", "payments", "customer_queue"}
STATES = {"draft", "rejected", "approved_pending_delivery", "published", "delivery_failed"}
WORD_MIN, WORD_MAX = 250, 500

@dataclass
class JournalResult:
    ok: bool
    status: str
    reason: str = ""
    entry_id: str = ""
    revision: int = 1
    content_hash: str = ""
    http_status: int = 0
    idempotent: bool = False


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _hash(*parts: Any) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()

def _json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))

def ensure_schema(db_path: str) -> None:
    with sqlite3.connect(db_path) as c:
        c.execute("""CREATE TABLE IF NOT EXISTS bnl_journal_entries (
            entry_id TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, guild_id INTEGER NOT NULL,
            lifecycle_state TEXT NOT NULL, title TEXT NOT NULL, excerpt TEXT NOT NULL,
            sections_json TEXT NOT NULL, public_payload_json TEXT, canonical_payload_bytes BLOB,
            content_hash TEXT NOT NULL, source_window_start TEXT NOT NULL, source_window_end TEXT NOT NULL,
            authored_at TEXT NOT NULL, approved_at TEXT, published_at TEXT, review_reason TEXT,
            delivery_status TEXT, delivery_http_status INTEGER DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            PRIMARY KEY(entry_id, revision))""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bnl_journal_state ON bnl_journal_entries(guild_id,lifecycle_state,created_at DESC)")
        c.execute("""CREATE TABLE IF NOT EXISTS bnl_journal_private_metadata (
            entry_id TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, guild_id INTEGER NOT NULL,
            metadata_json TEXT NOT NULL, content_hash TEXT NOT NULL, lifecycle_state TEXT NOT NULL,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(entry_id, revision))""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bnl_journal_meta_state ON bnl_journal_private_metadata(guild_id,lifecycle_state,updated_at DESC)")

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())

def _cols(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

def accepted_relays(conn, guild_id: int, start: str, end: str, limit: int = 20) -> list[dict[str, Any]]:
    if not table_exists(conn, "website_relay_history"): return []
    rows = conn.execute("""SELECT relay_id, public_message, public_directive, event_type, published_timestamp
        FROM website_relay_history WHERE guild_id=? AND published_timestamp>=? AND published_timestamp<=?
        ORDER BY published_timestamp DESC LIMIT ?""", (guild_id, start, end, limit)).fetchall()
    return [{"refId": f"relay:{r[0]}", "relayId": r[0], "summary": f"{r[1]} {r[2]}", "observedAt": r[4], "sourceType": "accepted_website_relay"} for r in rows]

def public_conversations(conn, guild_id: int, start: str, end: str, limit: int = 40) -> list[dict[str, Any]]:
    if not table_exists(conn, "conversations"): return []
    cols = _cols(conn, "conversations")
    public_usable_clause = " AND public_usable=1" if "public_usable" in cols else ""
    visibility_clause = " AND visibility IN ('public','public_safe')" if "visibility" in cols else ""
    rows = conn.execute(f"""SELECT id, user_id, user_name, channel_policy, channel_name, content, timestamp
        FROM conversations WHERE guild_id=? AND channel_policy IN ({','.join('?' for _ in PUBLIC_POLICIES)})
        AND timestamp>=? AND timestamp<=? {public_usable_clause} {visibility_clause}
        ORDER BY timestamp DESC LIMIT ?""", (guild_id, *sorted(PUBLIC_POLICIES), start, end, limit)).fetchall()
    out=[]
    for r in rows:
        text = sanitize_source_summary(r[5])
        if text:
            out.append({"refId": f"conversation:{r[0]}", "messageId": int(r[0]), "subjectRef": f"discord_user:{r[1]}", "channelPolicy": r[3], "channelName": r[4], "summary": text, "observedAt": r[6], "sourceType": "public_conversation"})
    return out

def sanitize_source_summary(text: str) -> str:
    text = re.sub(r"https?://\S+", "", text or "")
    text = re.sub(r"<@!?\d+>|@\w+", "someone", text)
    text = re.sub(r"\b\d{12,}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:240]

def tokenize(text: str) -> set[str]:
    return {w for w in re.findall(r"[a-z0-9]{4,}", (text or "").lower()) if w not in {"this","that","with","from","they","have","about","public"}}

def retrieve_history(db_path: str, guild_id: int, current_packet: dict[str, Any], limit: int = 6) -> dict[str, Any]:
    ensure_schema(db_path)
    terms = tokenize(_json(current_packet))
    with sqlite3.connect(db_path) as c:
        c.row_factory=sqlite3.Row
        prev = c.execute("SELECT entry_id,revision,title,excerpt,sections_json,created_at FROM bnl_journal_entries WHERE guild_id=? AND lifecycle_state='published' ORDER BY published_at DESC, created_at DESC LIMIT 1", (guild_id,)).fetchone()
        rows = c.execute("SELECT e.entry_id,e.revision,e.title,e.excerpt,e.sections_json,m.metadata_json,e.created_at FROM bnl_journal_entries e JOIN bnl_journal_private_metadata m ON m.entry_id=e.entry_id AND m.revision=e.revision WHERE e.guild_id=? AND e.lifecycle_state='published' AND m.lifecycle_state='published'", (guild_id,)).fetchall()
    scored=[]; counts={}; notes=[]
    for r in rows:
        meta=json.loads(r[5] or "{}")
        for t in meta.get("topicTags",[]): counts[t]=counts.get(t,0)+1
        hay=" ".join([r[2] or "", r[3] or "", r[4] or "", r[5] or ""])
        score=len(terms & tokenize(hay))
        if score: scored.append((score, dict(r)))
        if terms & set(meta.get("topicTags",[]) + meta.get("subjectRefs",[])):
            notes.extend(meta.get("continuityNotes",[])[:3])
    scored.sort(key=lambda x:(-x[0], x[1].get("created_at") or ""))
    return {"previousEntry": dict(prev) if prev else None, "relevantOlderEntries": [r for _,r in scored[:limit]], "recurringTopicCounts": counts, "matchingContinuityNotes": notes[:10]}

def build_source_packet(db_path: str, guild_id: int, hours: int = 72, now: Optional[str] = None) -> dict[str, Any]:
    end = now or utc_now_iso(); start = (datetime.fromisoformat(end.replace('Z','+00:00'))-timedelta(hours=max(1,min(int(hours),168)))).isoformat().replace('+00:00','Z')
    with sqlite3.connect(db_path) as c:
        relays=accepted_relays(c,guild_id,start,end); convos=public_conversations(c,guild_id,start,end)
    packet={"sourceWindowStart":start,"sourceWindowEnd":end,"relays":relays,"conversations":convos}
    packet["history"]=retrieve_history(db_path,guild_id,packet)
    packet["aggregateCounts"]={"eligibleRelays":len(relays),"eligibleConversations":len(convos),"participants":len({x.get('subjectRef') for x in convos}),"channels":len({x.get('channelName') for x in convos})}
    return packet

def generated_fallback(packet: dict[str,Any]) -> Optional[dict[str,Any]]:
    sources=packet.get('relays',[]) + packet.get('conversations',[])
    if not sources: return None
    refs=[s['refId'] for s in sources[:3]]
    body=("BARCODE's public rooms supplied BNL with a usable little knot of activity: accepted website relays on one side, "
          "and public conversation fragments on the other. The shape is not scandal. It is texture: people circling songs, "
          "bits, timing, and the small ritual weather that forms around a music community when everyone keeps adding one more strange brick. "
          "The prior Journal archive is treated as memory of earlier observations, not proof of today; it merely points to older echoes worth comparing. "
          "For this entry, the fresh public-safe sources carry the weight, and the older notes stay in the rafters like labeled wires.")
    return {"title":"A Small Machine Heard Through the Wall","excerpt":"BNL links recent public relay texture with the older Journal archive without exposing the wires.","sections":[{"heading":"Fresh Wires, Old Echoes","body":body}],"sourceRefIds":{"Fresh Wires, Old Echoes":refs},"metadata":{"topicTags":["music","community-patterns"],"continuityNotes":["Watch for recurring music-discussion callbacks in future public-safe windows."],"unresolvedQuestions":["Which public themes repeat after the website endpoint exists?"],"confidenceFlags":["grounded"],"safetyFlags":["public_copy_anonymous"]}}

def public_word_count(article):
    text=' '.join([article.get('title',''),article.get('excerpt','')] + [s.get('heading','')+' '+s.get('body','') for s in article.get('sections',[])])
    return len(re.findall(r"\b\w+\b", text))

def validate_article(article: dict[str,Any], packet: dict[str,Any], prior_titles: Optional[list[str]]=None, approved_names: Optional[set[str]]=None) -> str:
    sections=article.get('sections')
    if not isinstance(sections,list) or not (1<=len(sections)<=3): return 'invalid_section_count'
    wc=public_word_count(article)
    if wc<WORD_MIN or wc>WORD_MAX: return 'invalid_word_count'
    valid={s['refId'] for s in packet.get('relays',[])+packet.get('conversations',[])}
    if not valid: return 'no_new_source'
    refmap=article.get('sourceRefIds') or {}
    for sec in sections:
        refs=refmap.get(sec.get('heading')) or sec.get('sourceRefIds')
        if not refs or any(r not in valid for r in refs): return 'invalid_section_source_refs'
    public_text='\n'.join([article.get('title',''),article.get('excerpt','')] + [s.get('heading','')+'\n'+s.get('body','') for s in sections])
    if re.search(r"<@!?\d+>|@\w+|https?://|\b\d{12,}\b|relationship_journal|memory_tiers|source-file|dossier", public_text, re.I): return 'public_leak_pattern'
    for c in packet.get('conversations',[]):
        raw = c.get('summary','')
        if raw and len(raw.split()) >= 5 and raw.lower() in public_text.lower(): return 'discord_phrase_copy'
    names=set(approved_names or [])
    for c in packet.get('conversations',[]):
        nm=str(c.get('userName') or '').strip()
        if nm and nm not in names and re.search(r"\b"+re.escape(nm)+r"\b", public_text): return 'community_name_leak'
    norm= re.sub(r"\W+"," ", article.get('title','').lower()).strip()
    for t in prior_titles or []:
        if norm and norm == re.sub(r"\W+"," ", t.lower()).strip(): return 'duplicate_title'
    return ''

def build_public_payload(entry_id, revision, article, packet, content_hash, authored_at):
    return {"contractVersion":1,"kind":"journal_entry","entry":{"entryId":entry_id,"revision":revision,"title":article['title'].strip(),"excerpt":article['excerpt'].strip(),"sections":[{"heading":s['heading'].strip(),"body":s['body'].strip()} for s in article['sections']],"authoredAt":authored_at,"sourceWindowStart":packet['sourceWindowStart'],"sourceWindowEnd":packet['sourceWindowEnd'],"contentHash":content_hash}}

def create_draft(db_path: str, guild_id: int, hours: int = 72, generator: Callable[[dict[str,Any]], Optional[dict[str,Any]]]=generated_fallback) -> JournalResult:
    ensure_schema(db_path); packet=build_source_packet(db_path,guild_id,hours); article=generator(packet)
    if not article: return JournalResult(False,'no_draft','insufficient_grounded_material')
    with sqlite3.connect(db_path) as c:
        prior=[r[0] for r in c.execute("SELECT title FROM bnl_journal_entries WHERE guild_id=? AND lifecycle_state='published'",(guild_id,)).fetchall()]
    reason=validate_article(article,packet,prior)
    if reason: return JournalResult(False,'no_draft',reason)
    authored=utc_now_iso(); entry_id='journal_'+_hash(guild_id,authored,article['title'])[:16]; revision=1
    content_hash=_hash(article['title'],article['excerpt'],_json(article['sections']))
    payload=build_public_payload(entry_id,revision,article,packet,content_hash,authored); canonical=_json(payload).encode()
    meta=article.get('metadata',{}).copy(); meta.update({"sourceWindowStart":packet['sourceWindowStart'],"sourceWindowEnd":packet['sourceWindowEnd'],"supportingRelayIds":[r.get('relayId') for r in packet.get('relays',[])],"supportingConversationRefs":[{k:v for k,v in c.items() if k in {'refId','messageId','subjectRef','channelPolicy','observedAt'}} for c in packet.get('conversations',[])],"relatedPriorJournalEntryIds":[e.get('entry_id') for e in packet.get('history',{}).get('relevantOlderEntries',[])],"recurringTopicCounts":packet.get('history',{}).get('recurringTopicCounts',{}),"aggregateCounts":packet.get('aggregateCounts',{}),"sourceSummaries":[{"refId":s['refId'],"hash":_hash(s.get('summary','')),"summary":s.get('summary','')[:160]} for s in packet.get('relays',[])+packet.get('conversations',[])],"sourceRefIds":article.get('sourceRefIds',{})})
    now=utc_now_iso()
    with sqlite3.connect(db_path) as c:
        c.execute("INSERT INTO bnl_journal_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(entry_id,revision,guild_id,'draft',article['title'],article['excerpt'],_json(article['sections']),_json(payload),canonical,content_hash,packet['sourceWindowStart'],packet['sourceWindowEnd'],authored,None,None,None,None,0,now,now))
        c.execute("INSERT INTO bnl_journal_private_metadata VALUES (?,?,?,?,?,?,?,?)",(entry_id,revision,guild_id,_json(meta),content_hash,'draft',now,now))
    return JournalResult(True,'draft','',entry_id,revision,content_hash)

def approve_draft(db_path,guild_id,entry_id,content_hash):
    ensure_schema(db_path); now=utc_now_iso()
    with sqlite3.connect(db_path) as c:
        row=c.execute("SELECT lifecycle_state,content_hash FROM bnl_journal_entries WHERE guild_id=? AND entry_id=? ORDER BY revision DESC LIMIT 1",(guild_id,entry_id)).fetchone()
        if not row: return JournalResult(False,'not_found','not_found',entry_id)
        if row[0] != 'draft': return JournalResult(False,row[0],'not_draft',entry_id)
        if row[1] != content_hash: return JournalResult(False,'draft','content_hash_mismatch',entry_id,content_hash=row[1])
        c.execute("UPDATE bnl_journal_entries SET lifecycle_state='approved_pending_delivery',approved_at=?,updated_at=? WHERE guild_id=? AND entry_id=?",(now,now,guild_id,entry_id))
        c.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state='approved_pending_delivery',updated_at=? WHERE guild_id=? AND entry_id=?",(now,guild_id,entry_id))
    return JournalResult(True,'approved_pending_delivery','',entry_id,content_hash=content_hash)

def reject_draft(db_path,guild_id,entry_id,reason=''):
    ensure_schema(db_path); now=utc_now_iso()
    with sqlite3.connect(db_path) as c:
        c.execute("UPDATE bnl_journal_entries SET lifecycle_state='rejected',review_reason=?,updated_at=? WHERE guild_id=? AND entry_id=? AND lifecycle_state='draft'",(reason[:500],now,guild_id,entry_id))
        c.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state='rejected',updated_at=? WHERE guild_id=? AND entry_id=?",(now,guild_id,entry_id))
    return JournalResult(True,'rejected','',entry_id)

def deliver_approved(db_path, guild_id, entry_id, base_url, api_key, opener=None, timeout=10):
    ensure_schema(db_path); opener=opener or urllib.request.urlopen
    with sqlite3.connect(db_path) as c:
        row=c.execute("SELECT canonical_payload_bytes,content_hash FROM bnl_journal_entries WHERE guild_id=? AND entry_id=? AND lifecycle_state IN ('approved_pending_delivery','delivery_failed')",(guild_id,entry_id)).fetchone()
    if not row: return JournalResult(False,'not_deliverable','not_approved',entry_id)
    url=base_url.rstrip('/') + '/api/bnl/journal'; req=urllib.request.Request(url,data=row[0],method='POST',headers={'Content-Type':'application/json','x-api-key':api_key})
    status='delivery_failed'; reason='retryable_delivery_failure'; http=0; idem=False; published=''
    try:
        with opener(req, timeout=timeout) as resp:
            http=getattr(resp,'status',None) or resp.getcode(); data=json.loads(resp.read().decode() or '{}')
        if 200 <= http < 300 and data.get('ok') is True and data.get('persisted') is True:
            ack=data.get('entry') or {}; submitted=json.loads(row[0].decode())['entry']
            if ack.get('entryId')==submitted['entryId'] and ack.get('revision')==submitted['revision'] and ack.get('contentHash')==submitted['contentHash']:
                status='published'; reason=''; idem=bool(data.get('idempotent')); published=ack.get('publishedAt') or utc_now_iso()
            elif ack.get('entryId')==submitted['entryId'] and ack.get('revision')==submitted['revision']:
                reason='journal_id_conflict'
            else: reason='response_mismatch'
        elif http==404: reason='endpoint_not_found'
        elif http in (401,403): reason='authentication_failed'
        elif http==409: reason='journal_id_conflict'
        else: reason='http_rejected'
    except urllib.error.HTTPError as e:
        http=e.code; reason='endpoint_not_found' if e.code==404 else ('journal_id_conflict' if e.code==409 else 'http_rejected')
    except Exception:
        reason='retryable_delivery_failure'
    now=utc_now_iso()
    with sqlite3.connect(db_path) as c:
        c.execute("UPDATE bnl_journal_entries SET lifecycle_state=?,delivery_status=?,delivery_http_status=?,published_at=COALESCE(?,published_at),updated_at=? WHERE guild_id=? AND entry_id=?",(status,reason,http,published or None,now,guild_id,entry_id))
        c.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state=?,updated_at=? WHERE guild_id=? AND entry_id=?",(status,now,guild_id,entry_id))
    return JournalResult(status=='published',status,reason,entry_id,content_hash=row[1],http_status=http,idempotent=idem)

def preview(db_path,guild_id,entry_id=None):
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as c:
        c.row_factory=sqlite3.Row
        row=c.execute("SELECT entry_id,revision,lifecycle_state,title,excerpt,sections_json,content_hash FROM bnl_journal_entries WHERE guild_id=? " + ("AND entry_id=? " if entry_id else "") + "ORDER BY created_at DESC LIMIT 1", ((guild_id,entry_id) if entry_id else (guild_id,))).fetchone()
    return dict(row) if row else None
