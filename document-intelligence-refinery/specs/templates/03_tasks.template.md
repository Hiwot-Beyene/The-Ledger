# Tasks: [FEATURE NAME]

**Feature**: `[[###-feature-name]]`  
**Inputs**: `01_spec.md`, `02_design.md`, source PRD in `docs/`  
**Mode**: Execution-ready task planning

## Task Format

- `[ID] [P?] [Story/Phase] Description with explicit file path`
- `[P]` = can run in parallel

## Phase 1: Setup (Shared)

- [ ] T001 Create feature folder structure in `specs/[###-feature-name]/`
- [ ] T002 Define initial artifacts and conventions
- [ ] T003 [P] Prepare configuration/rules files

## Phase 2: Foundational (Blocking)

- [ ] T004 Define core schemas in `src/models/`
- [ ] T005 [P] Define stage interfaces in `src/agents/` or `src/strategies/`
- [ ] T006 [P] Define persistence/artifact policies
- [ ] T007 Define observability/audit requirements

**Checkpoint**: No story implementation starts until foundational tasks are complete.

## Phase 3: User Story 1 (P1) 🎯 MVP

**Goal**: [US1 goal]

**Independent Test**: [US1 test]

- [ ] T008 [US1] Define contracts needed by US1
- [ ] T009 [P] [US1] Define validation/test cases for US1
- [ ] T010 [US1] Define artifact outputs and acceptance checks

**Checkpoint**: US1 can be demonstrated independently.

## Phase 4: User Story 2 (P2)

**Goal**: [US2 goal]

**Independent Test**: [US2 test]

- [ ] T011 [US2] Define contracts needed by US2
- [ ] T012 [P] [US2] Define validation/test cases for US2
- [ ] T013 [US2] Define integration constraints with US1

## Phase 5: User Story 3 (P3)

**Goal**: [US3 goal]

**Independent Test**: [US3 test]

- [ ] T014 [US3] Define contracts needed by US3
- [ ] T015 [P] [US3] Define validation/test cases for US3
- [ ] T016 [US3] Define integration constraints with prior stories

## Phase 6: Polish & Delivery

- [ ] T017 [P] Documentation updates
- [ ] T018 Compliance checks against constitution
- [ ] T019 Final acceptance and demo/readiness checklist

## Dependencies & Execution Order

1. Setup → Foundational → User stories → Polish
2. User stories are independently testable by design
3. Parallel tasks marked `[P]` should avoid file conflicts

## Definition of Done

- Tasks map directly to stories and requirements in `01_spec.md`
- Contracts/design references are consistent with `02_design.md`
- Acceptance checks are explicit and measurable
- Output is ready for AI-assisted execution
