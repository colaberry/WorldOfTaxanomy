"""Tests for wiki loader and content integrity.

TDD RED phase: verifies wiki/_meta.json, wiki loader functions,
content structure, token budgets, and em-dash compliance.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


WIKI_DIR = Path(__file__).parent.parent / "wiki"


# -- Meta tests ---------------------------------------------------------------


class TestWikiMeta:
    def test_meta_json_exists(self):
        assert (WIKI_DIR / "_meta.json").exists(), "_meta.json not found in wiki/"

    def test_meta_json_is_valid(self):
        data = json.loads((WIKI_DIR / "_meta.json").read_text())
        assert isinstance(data, list)
        assert len(data) > 0

    def test_meta_entries_have_required_fields(self):
        data = json.loads((WIKI_DIR / "_meta.json").read_text())
        for entry in data:
            assert "slug" in entry, f"Missing 'slug' in {entry}"
            assert "file" in entry, f"Missing 'file' in {entry}"
            assert "title" in entry, f"Missing 'title' in {entry}"
            assert "description" in entry, f"Missing 'description' in {entry}"
            assert "order" in entry, f"Missing 'order' in {entry}"

    def test_all_referenced_files_exist(self):
        data = json.loads((WIKI_DIR / "_meta.json").read_text())
        for entry in data:
            fpath = WIKI_DIR / entry["file"]
            assert fpath.exists(), f"File {entry['file']} referenced in _meta.json does not exist"


# -- Loader tests --------------------------------------------------------------


class TestWikiLoader:
    def test_load_wiki_meta_returns_list(self):
        from world_of_taxonomy.wiki import load_wiki_meta
        meta = load_wiki_meta()
        assert isinstance(meta, list)
        assert len(meta) > 0

    def test_load_wiki_page_returns_content(self):
        from world_of_taxonomy.wiki import load_wiki_page
        content = load_wiki_page("getting-started")
        assert content is not None
        assert len(content) > 100

    def test_load_wiki_page_unknown_returns_none(self):
        from world_of_taxonomy.wiki import load_wiki_page
        assert load_wiki_page("nonexistent-page-xyz") is None

    def test_load_all_wiki_pages(self):
        from world_of_taxonomy.wiki import load_wiki_meta, load_all_wiki_pages
        meta = load_wiki_meta()
        pages = load_all_wiki_pages()
        assert isinstance(pages, dict)
        assert len(pages) == len(meta)
        for entry in meta:
            assert entry["slug"] in pages


# -- Context builder tests -----------------------------------------------------


class TestWikiContext:
    def test_build_wiki_context_non_empty(self):
        from world_of_taxonomy.wiki import build_wiki_context
        ctx = build_wiki_context()
        assert isinstance(ctx, str)
        assert len(ctx) > 100

    def test_build_wiki_context_under_budget(self):
        from world_of_taxonomy.wiki import build_wiki_context
        ctx = build_wiki_context()
        # 20K tokens ~ 80K chars (conservative estimate)
        assert len(ctx) < 80_000, f"Wiki context is {len(ctx)} chars, exceeds 80K budget"

    def test_build_llms_full_txt_includes_titles(self):
        from world_of_taxonomy.wiki import load_wiki_meta, build_llms_full_txt
        meta = load_wiki_meta()
        full = build_llms_full_txt()
        for entry in meta:
            assert entry["title"] in full, f"Title '{entry['title']}' missing from llms-full.txt"

    def test_build_llms_full_txt_ordered(self):
        from world_of_taxonomy.wiki import load_wiki_meta, build_llms_full_txt
        meta = load_wiki_meta()
        full = build_llms_full_txt()
        positions = []
        for entry in sorted(meta, key=lambda e: e["order"]):
            pos = full.find(entry["title"])
            assert pos >= 0, f"Title '{entry['title']}' not found"
            positions.append(pos)
        assert positions == sorted(positions), "Pages not in _meta.json order"


# -- Content integrity tests ---------------------------------------------------


class TestWikiContentIntegrity:
    def test_no_em_dashes_in_wiki_files(self):
        for md_file in WIKI_DIR.glob("*.md"):
            content = md_file.read_text()
            assert "\u2014" not in content, (
                f"Em-dash (U+2014) found in {md_file.name}"
            )

    def test_each_page_has_h2_heading(self):
        data = json.loads((WIKI_DIR / "_meta.json").read_text())
        for entry in data:
            content = (WIKI_DIR / entry["file"]).read_text()
            assert "\n## " in content or content.startswith("## "), (
                f"{entry['file']} has no ## heading"
            )

    def test_no_page_exceeds_token_budget(self):
        data = json.loads((WIKI_DIR / "_meta.json").read_text())
        for entry in data:
            content = (WIKI_DIR / entry["file"]).read_text()
            # ~10K tokens ~ 40K chars
            assert len(content) < 40_000, (
                f"{entry['file']} is {len(content)} chars, exceeds 40K budget"
            )

    def test_total_wiki_under_context_budget(self):
        total = 0
        for md_file in WIKI_DIR.glob("*.md"):
            total += len(md_file.read_text())
        # ~80K tokens ~ 320K chars
        assert total < 320_000, f"Total wiki is {total} chars, exceeds 320K budget"


# -- Diagram tests -------------------------------------------------------------


class TestWikiDiagrams:
    def test_architecture_page_exists(self):
        assert (WIKI_DIR / "architecture.md").exists()

    def test_architecture_has_mermaid_blocks(self):
        content = (WIKI_DIR / "architecture.md").read_text()
        count = content.count("```mermaid")
        assert count >= 4, f"Expected at least 4 mermaid blocks, found {count}"

    def test_diagram_source_files_exist(self):
        diagrams_dir = Path(__file__).parent.parent / "docs" / "diagrams"
        expected = [
            "system-architecture.mmd",
            "wiki-data-flow.mmd",
            "ingestion-pipeline.mmd",
            "api-request-flow.mmd",
            "mcp-session-lifecycle.mmd",
        ]
        for name in expected:
            assert (diagrams_dir / name).exists(), f"Missing {name} in docs/diagrams/"

    def test_mermaid_blocks_are_valid(self):
        content = (WIKI_DIR / "architecture.md").read_text()
        in_block = False
        first_lines = []
        for line in content.split("\n"):
            if line.strip() == "```mermaid":
                in_block = True
                continue
            if in_block and line.strip():
                first_lines.append(line.strip())
                in_block = False
        valid_starts = {
            "graph",
            "sequenceDiagram",
            "flowchart",
            "classDiagram",
            "stateDiagram",
            "erDiagram",
        }
        for fl in first_lines:
            keyword = fl.split()[0] if fl.split() else ""
            assert keyword in valid_starts, (
                f"Mermaid block starts with '{keyword}', expected one of {valid_starts}"
            )
