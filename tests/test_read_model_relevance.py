import os
import unittest

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


class ReadModelRelevanceTests(unittest.TestCase):
    def test_public_dossier_questions_trigger_read_model(self):
        relevant_examples = [
            "does the site have a dossier for Auxchord?",
            "does the website have a dossier for Signal Witch?",
            "is there a public dossier for Auxchord?",
            "show me the site dossier for Auxchord",
            "what dossier does the site have for Auxchord?",
        ]
        for text in relevant_examples:
            with self.subTest(text=text):
                self.assertTrue(bnl01_bot.is_bnl_read_model_relevant(text))

    def test_unrelated_dossier_sentence_does_not_trigger_read_model(self):
        text = "I keep a dossier of favorite synth patches at home."
        self.assertFalse(bnl01_bot.is_bnl_read_model_relevant(text))


if __name__ == "__main__":
    unittest.main()
