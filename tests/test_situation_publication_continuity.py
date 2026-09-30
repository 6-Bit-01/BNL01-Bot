import unittest

from bnl_unified_response_assessment import (
    build_situation_frame_v1,
    situation_request_clauses,
    situation_task_texts,
)


class SituationPublicationContinuityTests(unittest.TestCase):
    def frame(self, text, **overrides):
        values = dict(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="sealed_test",
            current_text=text,
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Speaker",),
            response_act="answer",
        )
        values.update(overrides)
        return build_situation_frame_v1(**values)

    def test_publication_setup_survives_into_dependent_request(self):
        for owner in ("Journal", "Relay"):
            text = (
                "BNL, I mean your most recently published %s. "
                "Pick one actual topic from it and tell me why you think "
                "it matters to the community." % owner
            )
            with self.subTest(owner=owner):
                frame = self.frame(text)
                self.assertEqual(len(frame.tasks), 1)
                task = frame.tasks[0]
                self.assertEqual(task.object_kind, owner.lower())
                self.assertEqual(task.task_kind, "retrieve_publication")
                self.assertEqual(task.authority_scope, "packet")
                self.assertEqual(task.subject_indexes, ())
                self.assertIn("most recently published " + owner,
                              situation_request_clauses(text)[0])
                self.assertEqual(situation_task_texts(frame, current_text=text),
                                 (text.rstrip("."),))

    def test_dependent_publication_followup_keeps_immediate_owner(self):
        text = "Summarize your latest Journal. Explain why it matters."
        frame = self.frame(text)
        self.assertEqual(len(frame.tasks), 2)
        self.assertEqual(tuple(task.object_kind for task in frame.tasks),
                         ("journal", "journal"))
        self.assertTrue(all(task.authority_scope == "packet" for task in frame.tasks))
        self.assertEqual(situation_task_texts(frame, current_text=text),
                         ("Summarize your latest Journal", "Explain why it matters"))

        switched = (
            "Summarize your latest Journal. Summarize your latest Relay. "
            "Explain why that topic matters."
        )
        frame = self.frame(switched)
        self.assertEqual(tuple(task.object_kind for task in frame.tasks),
                         ("journal", "relay", "relay"))
        self.assertNotIn("Journal", situation_request_clauses(switched)[-1])

    def test_mixed_boundary_request_keeps_why_with_publication(self):
        text = (
            "BNL, we can put that rough exchange behind us. "
            "The limit on teasing still stands. Which part of your latest "
            "published Journal deserves another conversation, and why?"
        )
        frame = self.frame(text)
        self.assertEqual(len(frame.tasks), 2)
        self.assertEqual(tuple(task.object_kind for task in frame.tasks),
                         ("journal", "journal"))
        self.assertEqual(tuple(task.authority_scope for task in frame.tasks),
                         ("packet", "packet"))
        self.assertEqual(situation_task_texts(frame, current_text=text)[-1], "why")

    def test_independent_tasks_do_not_borrow_publication_setup(self):
        for question, owner, authority in (
            ("Where is Seattle?", "unknown", "external_public"),
            ("Tell me about Test Member.", "person", "packet"),
            ("What is Seattle's weather today?", "unknown", "external_current"),
            ("Summarize your latest Relay.", "relay", "packet"),
            ("What is your own take on Neptune?", "unknown", "external_public"),
            ("Tell me how the Earth makes it through a year.", "unknown", "external_public"),
            ("Explain why Seattle calls it rain.", "unknown", "external_public"),
        ):
            text = "I read your Journal. " + question
            with self.subTest(question=question):
                frame = self.frame(text, subject_label_hints=("Test Member",))
                self.assertEqual(len(frame.tasks), 1)
                self.assertEqual(frame.tasks[0].object_kind, owner)
                self.assertEqual(frame.tasks[0].authority_scope, authority)
                self.assertNotIn("Journal", situation_request_clauses(text)[0])

    def test_publication_context_does_not_cross_unrelated_task_or_setup(self):
        for text in (
            "Summarize your latest Journal. Explain how stars form. What is your own take?",
            "I read your Journal. The weather has changed. Explain why it matters.",
        ):
            with self.subTest(text=text):
                frame = self.frame(text)
                self.assertEqual(frame.tasks[-1].object_kind, "unknown")
                self.assertNotIn("Journal", situation_request_clauses(text)[-1])

    def test_member_scope_stays_with_publication_not_incidental_setup(self):
        member_text = (
            "I mean your latest Journal about Test Member. "
            "Explain what it says."
        )
        incidental_text = (
            "Test Member is here. I mean your latest Journal. "
            "Explain what it says."
        )
        for text, indexes in ((member_text, (0,)), (incidental_text, ())):
            with self.subTest(text=text):
                frame = self.frame(text, subject_label_hints=("Test Member",))
                self.assertEqual(frame.tasks[-1].object_kind, "journal")
                self.assertEqual(frame.tasks[-1].subject_indexes, indexes)

    def test_competing_publication_setup_does_not_choose_an_owner(self):
        for setup in ("I read your Journal and Relay", "I mean that Journal entry"):
            text = setup + ". Explain why it matters."
            with self.subTest(setup=setup):
                frame = self.frame(text)
                self.assertEqual(frame.tasks[-1].object_kind, "unknown")
                self.assertEqual(situation_request_clauses(text),
                                 ("Explain why it matters",))

    def test_publication_antecedent_preserves_explicit_window(self):
        text = (
            "I mean your Journal from September 25. "
            "Explain why its main topic matters."
        )
        frame = self.frame(text)
        self.assertEqual(frame.tasks[0].object_kind, "journal")
        self.assertIn("Journal from September 25", situation_request_clauses(text)[0])

    def test_publication_setup_does_not_replace_requested_action(self):
        text = "Your latest Journal was completed today. Explain why it matters."
        frame = self.frame(text)
        self.assertEqual(frame.tasks[0].object_kind, "journal")
        self.assertEqual(frame.tasks[0].task_kind, "retrieve_publication")
        self.assertEqual(frame.tasks[0].currentness, "current")


if __name__ == "__main__":
    unittest.main()
