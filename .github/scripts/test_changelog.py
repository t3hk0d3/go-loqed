import unittest

from changelog import ChangelogError, addon, release, released, section

SAMPLE = """# Changelog

## [Unreleased]

### Fixed

- A thing.

## [0.1.1] - 2026-10-07

### Fixed

- Another thing.

[Unreleased]: https://github.com/o/r/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/o/r/compare/v0.1.0...v0.1.1
"""


class ReleaseTest(unittest.TestCase):
    def test_moves_unreleased_changes_under_the_version(self):
        out = release(SAMPLE, "0.1.2", "2026-10-09")
        self.assertEqual(section(out, "0.1.2"), "### Fixed\n\n- A thing.")
        self.assertIn("## [0.1.2] - 2026-10-09", out)

    def test_starts_an_empty_unreleased_section(self):
        out = release(SAMPLE, "0.1.2", "2026-10-09")
        self.assertEqual(section(out, "Unreleased"), "")
        self.assertLess(out.index("## [Unreleased]"), out.index("## [0.1.2]"))

    def test_updates_the_compare_links(self):
        out = release(SAMPLE, "0.1.2", "2026-10-09")
        self.assertIn("[Unreleased]: https://github.com/o/r/compare/v0.1.2...HEAD\n"
                      "[0.1.2]: https://github.com/o/r/compare/v0.1.1...v0.1.2\n"
                      "[0.1.1]: https://github.com/o/r/compare/v0.1.0...v0.1.1", out)

    def test_keeps_older_sections_unchanged(self):
        out = release(SAMPLE, "0.1.2", "2026-10-09")
        self.assertEqual(section(out, "0.1.1"), "### Fixed\n\n- Another thing.")

    def test_refuses_an_empty_unreleased_section(self):
        empty = release(SAMPLE, "0.1.2", "2026-10-09")
        with self.assertRaisesRegex(ChangelogError, "empty"):
            release(empty, "0.1.3", "2026-10-10")

    def test_refuses_an_existing_version(self):
        with self.assertRaisesRegex(ChangelogError, "already exists"):
            release(SAMPLE, "0.1.1", "2026-10-09")

    def test_refuses_a_missing_unreleased_link(self):
        text = SAMPLE.replace("[Unreleased]: https://github.com/o/r/compare/v0.1.1...HEAD\n", "")
        with self.assertRaisesRegex(ChangelogError, "link"):
            release(text, "0.1.2", "2026-10-09")


class SectionTest(unittest.TestCase):
    def test_last_section_stops_at_the_links(self):
        self.assertNotIn("[Unreleased]:", section(SAMPLE, "0.1.1"))

    def test_missing_section_is_an_error(self):
        with self.assertRaisesRegex(ChangelogError, "no ## \\[9.9.9\\]"):
            section(SAMPLE, "9.9.9")


class ReleasedTest(unittest.TestCase):
    def test_drops_only_the_unreleased_changes(self):
        text = released(SAMPLE)
        self.assertIn("## [Unreleased]", text)
        self.assertEqual(section(text, "Unreleased"), "")
        self.assertEqual(section(text, "0.1.1"), section(SAMPLE, "0.1.1"))
        self.assertEqual(text.split("## [0.1.1]")[1], SAMPLE.split("## [0.1.1]")[1])


class AddonTest(unittest.TestCase):
    def test_has_no_unreleased_section_or_link(self):
        text = addon(SAMPLE)
        self.assertNotIn("Unreleased", text)
        self.assertNotIn("A thing.", text)

    def test_keeps_the_released_versions_and_their_links(self):
        text = addon(SAMPLE)
        self.assertEqual(section(text, "0.1.1"), section(SAMPLE, "0.1.1"))
        self.assertIn("[0.1.1]: https://github.com/o/r/compare/v0.1.0...v0.1.1", text)
        self.assertTrue(text.startswith("# Changelog\n\n## [0.1.1]"), text)


class RepositoryChangelogTest(unittest.TestCase):
    def test_the_repository_changelog_can_be_released(self):
        with open("CHANGELOG.md", encoding="utf-8") as f:
            text = f.read()
        if section(text, "Unreleased"):
            release(text, "999.0.0", "2026-01-01")

    def test_the_addon_changelog_has_the_released_history(self):
        # Home Assistant shows addon/CHANGELOG.md in the add-on's update
        # dialog; the release workflow copies CHANGELOG.md there.
        with open("CHANGELOG.md", encoding="utf-8") as f:
            root = f.read()
        with open("addon/CHANGELOG.md", encoding="utf-8") as f:
            addon_text = f.read()
        self.assertEqual(addon_text, addon(root),
                         "addon/CHANGELOG.md is generated: run python3 .github/scripts/changelog.py addon")


if __name__ == "__main__":
    unittest.main()
