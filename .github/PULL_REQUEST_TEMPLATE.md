## Summary

<!-- One sentence: what does this PR add or fix? -->

## Type

- [ ] New classification system
- [ ] Bug fix
- [ ] Frontend change
- [ ] API change
- [ ] Documentation
- [ ] Other: ___

---

## For new classification systems - checklist

- [ ] Test file written FIRST and confirmed failing (Red) before any implementation
- [ ] All three tests pass (Green): `test_ingest_creates_system`, `test_ingest_creates_nodes`, `test_idempotent`
- [ ] System registered in `world_of_taxonomy/__main__.py` (dispatch block + choices list)
- [ ] `python3 -c "import ast; ast.parse(open('world_of_taxonomy/__main__.py').read()); print('OK')"` passes
- [ ] `CLAUDE.md` systems table updated with new row
- [ ] `DATA_SOURCES.md` updated with source URL and license
- [ ] No em-dash characters (U+2014) anywhere in changed files
- [ ] Full test suite passes: `python3 -m pytest tests/ -q`

## System details (if applicable)

| Field | Value |
|-------|-------|
| System ID | |
| Display name | |
| Region | |
| Codes ingested | |
| Source URL | |
| License | |
| Derived from | (NACE / ISIC / standalone) |

---

## Testing

```bash
# How to verify this PR works
python3 -m pytest tests/test_ingest_<system>.py -v
```
