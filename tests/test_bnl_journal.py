import io, json, sqlite3, tempfile, urllib.error, unittest
from contextlib import contextmanager

import bnl_journal as j



def good_article(packet):
    refs=[s['refId'] for s in packet['relays']+packet['conversations']]
    body=" ".join(["BARCODE's public music orbit produced a fresh, grounded pattern for BNL to inspect." for _ in range(30)])
    return {"title":"A New Coil in the BARCODE Room","excerpt":"BNL observes music talk, community rhythm, and old echoes without exposing private wires.","sections":[{"heading":"Public Noise, Carefully Labeled","body":body}],"sourceRefIds":{"Public Noise, Carefully Labeled":refs[:1]},"metadata":{"topicTags":["music","callback"],"subjectRefs":["discord_user:7"],"continuityNotes":["music callback"]}}

class Resp:
    status=200
    def __init__(self, body): self.body=body
    def read(self): return self.body
    def getcode(self): return self.status
    def __enter__(self): return self
    def __exit__(self,*a): return False

class JournalTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.NamedTemporaryFile(delete=False); self.db=self.tmp.name; self.tmp.close(); j.ensure_schema(self.db)
        with sqlite3.connect(self.db) as c:
            c.execute("CREATE TABLE website_relay_history(relay_id TEXT PRIMARY KEY,guild_id INTEGER,public_message TEXT,public_directive TEXT,mode TEXT,relay_lane TEXT,event_type TEXT,highest_source_conversation_id INTEGER,normalized_message TEXT,semantic_family TEXT,published_timestamp TEXT)")
            c.execute("CREATE TABLE conversations(id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,channel_name TEXT,channel_policy TEXT,role TEXT,content TEXT,timestamp TEXT,public_usable INTEGER,visibility TEXT)")
            c.execute("INSERT INTO website_relay_history VALUES (?,?,?,?,?,?,?,?,?,?,?)",('r1',1,'A track was discussed.','Keep listening.','OBS','lane','accepted',1,'a track','public','2026-07-18T00:00:00Z'))
            rows=[(1,7,'KnownUser',1,'general','public_home','user','people discuss bass chaos and a hook', '2026-07-18T00:01:00Z',1,'public_safe'),(2,8,'Private',1,'mods','internal_controlled','user','secret internal', '2026-07-18T00:02:00Z',1,'public_safe'),(3,9,'Nope',1,'general','public_home','user','not usable', '2026-07-18T00:03:00Z',0,'public_safe')]
            c.executemany("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
            c.execute("CREATE TABLE relationship_journal(id INTEGER,user_id INTEGER,guild_id INTEGER,entry_type TEXT,summary TEXT,timestamp TEXT)")
            c.execute("INSERT INTO relationship_journal VALUES (1,7,1,'state','private relationship dirt','2026-07-18T00:00:00Z')")
    def test_source_policy_public_usable_and_categorical_exclusions(self):
        packet=j.build_source_packet(self.db,1,24,'2026-07-18T01:00:00Z')
        self.assertEqual([c['messageId'] for c in packet['conversations']],[1])
        self.assertNotIn('relationship_journal', json.dumps(packet))
        self.assertNotIn('secret internal', json.dumps(packet))
    def test_complete_history_retrieval_and_rejected_excluded(self):
        with sqlite3.connect(self.db) as c:
            for i in range(12):
                state='published' if i!=10 else 'rejected'; eid=f'e{i}'; meta={"topicTags":["bass" if i==0 else "other"],"continuityNotes":[f'note{i}'],"subjectRefs":["discord_user:7"]}
                c.execute("INSERT INTO bnl_journal_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(eid,1,1,state,f'Title {i}','Excerpt','[]','{}',b'{}',f'h{i}','s','e','2026',None,f'2026-07-{i+1:02d}T00:00:00Z' if state=='published' else None,None,None,0,f'2026-07-{i+1:02d}T00:00:00Z',f'2026-07-{i+1:02d}T00:00:00Z'))
                c.execute("INSERT INTO bnl_journal_private_metadata VALUES (?,?,?,?,?,?,?,?)",(eid,1,1,json.dumps(meta),f'h{i}',state,'2026','2026'))
        hist=j.retrieve_history(self.db,1,{"bass":"discord_user:7"})
        self.assertEqual(hist['previousEntry']['entry_id'],'e11')
        self.assertTrue(any(r['entry_id']=='e0' for r in hist['relevantOlderEntries']))
        self.assertFalse(any(r['entry_id']=='e10' for r in hist['relevantOlderEntries']))
    def test_validation_limits_leaks_refs_duplicates_and_old_not_current(self):
        packet=j.build_source_packet(self.db,1,24,'2026-07-18T01:00:00Z')
        a=good_article(packet)
        self.assertEqual('', j.validate_article(a,packet,[]))
        bad=dict(a); bad['sections']=[]; self.assertEqual('invalid_section_count', j.validate_article(bad,packet,[]))
        bad=good_article(packet); bad['sections'][0]['body']='short'; self.assertEqual('invalid_word_count', j.validate_article(bad,packet,[]))
        bad=good_article(packet); bad['sourceRefIds']={'Public Noise, Carefully Labeled':['conversation:999']}; self.assertEqual('invalid_section_source_refs', j.validate_article(bad,packet,[]))
        bad=good_article(packet); bad['sections'][0]['body'] += ' @KnownUser https://x.test 1234567890123'; self.assertEqual('public_leak_pattern', j.validate_article(bad,packet,[]))
        bad=good_article(packet); bad['title']='A New Coil in the BARCODE Room'; self.assertEqual('duplicate_title', j.validate_article(bad,packet,['A New Coil in the BARCODE Room']))
    def test_private_metadata_public_payload_hash_approval_retry_and_404(self):
        res=j.create_draft(self.db,1,24,good_article); self.assertTrue(res.ok)
        with sqlite3.connect(self.db) as c:
            payload=json.loads(c.execute("SELECT public_payload_json FROM bnl_journal_entries").fetchone()[0]); meta=c.execute("SELECT metadata_json FROM bnl_journal_private_metadata").fetchone()[0]
        self.assertNotIn('supportingConversationRefs', json.dumps(payload)); self.assertIn('supportingConversationRefs', meta)
        self.assertFalse(j.approve_draft(self.db,1,res.entry_id,'bad').ok)
        self.assertTrue(j.approve_draft(self.db,1,res.entry_id,res.content_hash).ok)
        captured=[]
        def opener(req, timeout=10): captured.append(req.data); submitted=json.loads(req.data.decode())['entry']; return Resp(json.dumps({"ok":True,"persisted":True,"idempotent":True,"entry":{"entryId":submitted['entryId'],"revision":submitted['revision'],"contentHash":submitted['contentHash'],"publishedAt":"2026-07-18T01:00:00Z"}}).encode())
        d=j.deliver_approved(self.db,1,res.entry_id,'https://site.example','k',opener); self.assertTrue(d.ok and d.idempotent)
        self.assertEqual(captured[0], captured[0])
    def test_404_leaves_delivery_failed_truthfully(self):
        res=j.create_draft(self.db,1,24,good_article); j.approve_draft(self.db,1,res.entry_id,res.content_hash)
        def opener(req, timeout=10): raise urllib.error.HTTPError(req.full_url,404,'not found',{},io.BytesIO())
        d=j.deliver_approved(self.db,1,res.entry_id,'https://site.example','k',opener)
        self.assertFalse(d.ok); self.assertEqual(d.reason,'endpoint_not_found')
        with sqlite3.connect(self.db) as c: self.assertEqual(c.execute("SELECT lifecycle_state FROM bnl_journal_entries WHERE entry_id=?",(res.entry_id,)).fetchone()[0],'delivery_failed')

if __name__ == '__main__': unittest.main()
