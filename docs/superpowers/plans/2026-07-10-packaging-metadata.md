# Packaging Metadata Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove packaging metadata compatibility warnings without changing runtime behavior or local data handling.

**Architecture:** Modify only PEP 621 metadata in `pyproject.toml`. The package contents, command-line entry point, dependencies, test configuration, and `.gitignore` remain unchanged.

**Tech Stack:** Python 3.10+, setuptools, wheel, pytest, conda environment `agent`.

## Global Constraints

- Preserve `.gitignore` handling for the complete `data/` directory.
- Do not add runtime dependencies or alter parsing behavior.
- Run all verification through `conda run -n agent`.

---

### Task 1: Modernize PEP 621 metadata

**Files:**
- Modify: `pyproject.toml:11-22`

**Interfaces:**
- Consumes: Existing PEP 621 project metadata.
- Produces: A wheel build with valid metadata and no warnings about the license field or documentation URL.

- [x] **Step 1: Define the expected metadata**

The `[project]` section must express the Apache-2.0 license as a string and
the project must name `LICENSE` as its license file. The `[project.urls]`
section must not contain a repository-relative documentation path.

- [x] **Step 2: Update the metadata**

Replace:

```toml
license = { text = "Apache-2.0" }
```

with:

```toml
license = "Apache-2.0"
license-files = ["LICENSE"]
```

Remove:

```toml
[project.urls]
Documentation = "docs/local-file-parser-mvp-design.md"
```

- [x] **Step 3: Verify the distribution build**

Run:

```bash
conda run -n agent python -m build --wheel
```

Expected: exit code 0, and no warnings about `project.license` deprecation or
a documentation URL without a scheme.

- [x] **Step 4: Run the full test suite**

Run:

```bash
conda run -n agent python -m pytest -q
```

Expected: exit code 0 with every test passing.
