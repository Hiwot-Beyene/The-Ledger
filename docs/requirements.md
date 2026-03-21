# 🧠 THE LEDGER — PROJECT BRAIN (Cursor Reference)

## 📌 PURPOSE

This system is an **event-sourced, multi-agent decision platform**.

* Events = **single source of truth**
* Aggregates = **state reconstructed from events**
* Command Handlers = **write logic**
* Projections = **read models (CQRS)**
* Agents = **event producers with full audit trail**

---

# 🧱 CORE ARCHITECTURE

## 🔹 Event Sourcing Principle

State is NOT stored directly.

```
Current State = Replay(All Past Events)
```

* Events are immutable
* No updates or deletes
* Full auditability guaranteed

---

## 🔹 CQRS Separation

| Side         | Responsibility            |
| ------------ | ------------------------- |
| Command Side | Validate → produce events |
| Query Side   | Read from projections     |

❗ Command side MUST NOT read from projections

---

## 🔹 System Layers

1. Command/API Layer
2. Command Handlers
3. Aggregates (Domain Logic)
4. Event Store (Persistence)
5. Projection Daemon
6. Read Models / Query API
7. AI Agents (event producers)

---

# 🗄️ PHASE 1 — EVENT STORE CORE

## 🔹 Required Tables

### events

* event_id (UUID)
* stream_id
* stream_position (per stream ordering)
* global_position (global ordering)
* event_type
* event_version
* payload (JSONB)
* metadata (JSONB)
* recorded_at

Constraint:

* UNIQUE(stream_id, stream_position)

---

### event_streams

* stream_id (PK)
* aggregate_type
* current_version
* created_at
* archived_at
* metadata

---

### projection_checkpoints

* projection_name (PK)
* last_position
* updated_at

---

### outbox

* id (UUID)
* event_id (FK)
* destination
* payload
* created_at
* published_at
* attempts

---

## 🔹 Indexing Strategy (Critical)

Indexes must support:

1. Load stream → (stream_id, stream_position)
2. Replay all → (global_position)
3. Filter by type → (event_type)
4. Time queries → (recorded_at)

---

## 🔹 Lifecycle Support

* archived_at → for stream archival
* outbox tracking → delivery guarantees

---

# ⚙️ EVENT STORE IMPLEMENTATION

## 🔹 Required Methods

* append()
* load_stream()
* load_all()
* stream_version()
* archive_stream()
* get_stream_metadata()

---

## 🔹 append() Requirements

* Atomic transaction:

  * insert into events
  * insert into outbox
* Enforce expected_version at DB level
* Raise OptimisticConcurrencyError if mismatch

---

## 🔹 Optimistic Concurrency (OCC)

Rule:

```
IF current_version != expected_version
→ THROW OptimisticConcurrencyError
```

Test requirements:

* Two concurrent writers
* Only ONE succeeds
* Assertions:

  * total events correct
  * correct stream_position
  * error raised for loser

---

# 🧾 DOMAIN MODELS

## 🔹 BaseEvent vs StoredEvent

### BaseEvent (Domain)

* business fields only

### StoredEvent (Persistence)

* event_id
* stream_position
* global_position
* metadata

---

## 🔹 Exception Models

### OptimisticConcurrencyError

Fields:

* stream_id
* expected_version
* actual_version

### DomainError

* Used for business rule violations

---

## 🔹 StreamMetadata

* stream_id
* aggregate_type
* current_version
* archived_at

---

## 🔹 Event Catalogue

At least **8+ typed events required**

Examples:

* ApplicationSubmitted
* CreditAnalysisCompleted
* FraudScreeningCompleted
* ComplianceRulePassed
* DecisionGenerated
* ApplicationApproved
* ApplicationDeclined

---

# 🧠 PHASE 2 — AGGREGATES

## 🔹 Core Rule

Aggregates MUST:

* Rebuild state via event replay ONLY
* Never read external DB
* Enforce business rules

---

## 🔹 Aggregate Pattern

```
events = load_stream()
for event in events:
    apply(event)
```

---

## 🔹 Dispatch Pattern (Required)

```
handler = getattr(self, f"_on_{event_type}")
```

❗ No if/else chains

---

## 🔹 LoanApplication Aggregate

### Responsibilities:

* lifecycle state machine
* enforce transitions
* validate decisions

### States:

* SUBMITTED
* AWAITING_ANALYSIS
* ANALYSIS_COMPLETE
* COMPLIANCE_REVIEW
* PENDING_DECISION
* APPROVED / DECLINED

---

## 🔹 Business Rules (Critical)

1. No invalid state transitions
2. Cannot approve without compliance
3. Confidence < 0.6 → must REFER
4. No duplicate analysis unless overridden
5. Causal chain must be valid

---

## 🔹 AgentSession Aggregate

Rules:

* First event MUST be AgentContextLoaded (Gas Town)
* Model version must match
* Context must exist before decision

---

# 🔄 COMMAND HANDLER PATTERN

## 🔹 REQUIRED STRUCTURE

```
1. Load aggregate(s)
2. Validate (guard methods)
3. Create events (pure logic)
4. Append events
```

---

## 🔹 Requirements

* expected_version = aggregate.version
* NO hardcoding versions
* Accept and pass:

  * correlation_id
  * causation_id

---

## 🔹 Multi-Aggregate Loading

Some handlers MUST load:

* LoanApplication
* AgentSession

---

# 🤖 AGENT ARCHITECTURE

## 🔹 Agents

* DocumentProcessingAgent
* CreditAnalysisAgent
* FraudDetectionAgent
* ComplianceAgent
* DecisionOrchestrator

---

## 🔹 Agent Execution Flow

```
validate_inputs
→ open_aggregate_record
→ load_external_data
→ domain_logic (LLM)
→ write_output
```

---

## 🔹 Agent Session Events

* AgentSessionStarted (FIRST)
* AgentInputValidated
* AgentNodeExecuted (per node)
* AgentToolCalled
* AgentOutputWritten
* AgentSessionCompleted

---

## 🔹 Gas Town Pattern

* Agent writes event BEFORE doing work
* Enables crash recovery via replay

---

# 📊 PHASE 3 — PROJECTIONS

## 🔹 Purpose

Convert events → queryable data

---

## 🔹 Required Projections

### 1. ApplicationSummary

* current state per application

### 2. AgentPerformanceLedger

* metrics per agent

### 3. ComplianceAuditView (CRITICAL)

* full audit trail
* supports time-travel queries

---

## 🔹 Projection Daemon

Responsibilities:

* Read events by global_position
* Update projections
* Track checkpoints
* Handle failures gracefully

---

## 🔹 Lag Requirement

* ApplicationSummary < 500ms
* ComplianceAudit < 2s

---

## 🔹 Required Methods

* get_current_state()
* get_state_at(timestamp)
* rebuild_from_scratch()
* get_projection_lag()

---

# 🔐 PHASE 4 — ADVANCED

## 🔹 Upcasting

* Handle schema evolution
* Transform old events → new format
* Applied at read time

---

## 🔹 Outbox Pattern

* Prevent dual-write problem
* Guarantees event delivery

---

## 🔹 Audit Integrity

* Hash chain for tamper detection
* Required for compliance

---

# 🔗 SYSTEM FLOW

```
User → Command → Handler → Aggregate → EventStore
→ Projections → Query API → User
```

Agents:

```
Agents → Events → Event Store → Trigger next steps
```

---

# 🚨 CRITICAL RULES (DO NOT BREAK)

* Event store is the ONLY source of truth
* No direct DB writes outside event store
* No reading projections in command handlers
* No mutable state in aggregates
* All writes must go through append()

---

# 🧩 IMPLEMENTATION ORDER

1. Database schema
2. EventStore class
3. Event + Exception models
4. Aggregates
5. Command handlers
6. Projection daemon
7. Agents

---

# 🎯 SUCCESS CRITERIA

System must support:

* Full event replay
* Concurrent writes with OCC
* Accurate projections
* Complete audit trail
* Deterministic state reconstruction

---

# 🧠 MENTAL MODEL

Think of the system as:

* Event Store = Brain (memory)
* Aggregates = Reasoning
* Command Handlers = Decisions
* Projections = Views
* Agents = Workers

---
