# Spec Templates

This folder contains standardized templates for creating new feature specs.

## Files

- `01_spec.template.md` — user stories, Given/When/Then acceptance scenarios, requirements, entities, success criteria.
- `02_design.template.md` — architecture, schema-level data design, routing/decision logic, contracts, risks.
- `03_tasks.template.md` — phased execution plan with dependencies and independent story checkpoints.

## Recommended Usage

1. Create new feature folder under `specs/` with numeric prefix (example: `002-some-feature`).
2. Copy templates into the new folder and rename to:
   - `01_spec.md`
   - `02_design.md`
   - `03_tasks.md`
3. Fill content from the corresponding PRD in `docs/`.
4. Verify alignment with `.specify/memory/constitution.md`.

## Naming Convention

- Feature folder format: `NNN-kebab-case-feature-name`
- Example: `001-document-intelligence-refinery-mvp`
