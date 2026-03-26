# DOMAIN_NOTES

**Author:** Hiwot Beyene

## Context

In my assessment, this challenge is fundamentally about replacing ephemeral agent traces and mutable workflow state with a durable event-sourced ledger that can satisfy enterprise audit, temporal replay, concurrency control, and regulatory explainability requirements. The architecture in the provided materials is explicit about that shift. The write side is an append-only event store backed by PostgreSQL. The read side is a set of rebuildable projections maintained by an async daemon. The domain is partitioned into four aggregates, each of which represents a consistency boundary rather than a foldering convenience: `LoanApplication`, `AgentSession`, `ComplianceRecord`, and `AuditLedger`. Guards, invariants, and transition diagrams for the command-side aggregates live in `src/aggregates/README.md`.

The importance of this framing is that the problem is not "how do I log more details?" The real problem is "how do I make the event history itself the source of truth so that any current or historical state can be reconstructed, justified, and defended?" That distinction shapes every answer below.

## 1. EDA vs. Event Sourcing and the Concrete Redesign

A callback-based mechanism such as LangChain traces, LangGraph callbacks, or any similar event-like hook system is Event-Driven Architecture, not Event Sourcing. I am making that distinction on first principles, not on terminology. In EDA, events are messages that notify downstream components that something happened. They are propagation artifacts. The authoritative state usually still lives somewhere else, such as a mutable relational table, a cache, or a process-local object. If the message is delayed, dropped, deduplicated, or partially consumed, the rest of the system may still continue operating, because the event is not the database.

In Event Sourcing, the situation is different in kind, not merely in degree. The event stream is the authoritative persistence model. The event log is not commentary about state; it is the definition of state. If an event is lost, the system’s history is incomplete and state reconstruction becomes impossible. That is why the practitioner manual correctly distinguishes EDA as notification-oriented and Event Sourcing as persistence-oriented. EDA focuses on propagating state changes. Event Sourcing focuses on defining state through immutable history.

If I redesign the callback-based tracing architecture using The Ledger, three concrete architectural changes occur.

First, I move the system of record from mutable state plus callback exhaust to an append-only event store. For example, instead of a credit-analysis component updating an `applications` row and then emitting a trace, the command side would append domain facts such as `CreditAnalysisRequested`, `CreditAnalysisCompleted`, `DecisionGenerated`, or `ApplicationApproved` to the correct aggregate streams with durable `stream_position`, `global_position`, `event_version`, and causal metadata. The trace is no longer the evidence. The appended domain event is the evidence.

Second, I introduce a CQRS boundary that is enforced structurally. Commands become the only way to change state. A command handler loads the aggregate by replaying its event stream, validates invariants in domain logic, determines the new events, and appends them atomically. Queries never interrogate the command-side aggregate state directly during write processing. They read from projections built from events. This is an important discipline because the documents are explicit that commands must not validate against stale projections.

Third, I replace transient agent memory with durable agent memory. In the callback-based design, an agent session can lose critical context on restart, producing the "Gas Town" failure mode described in the challenge. In The Ledger design, `AgentSession` events such as `AgentContextLoaded`, `AgentInputValidated`, `AgentToolCalled`, `AgentOutputWritten`, and `AgentSessionCompleted` become durable memory. If the process crashes, the agent reconstructs its context by replaying its own stream.

The redesign produces concrete gains.

- I gain reproducibility because any historical state can be recomputed by replaying the stream.
- I gain auditability because every decision is anchored to immutable facts rather than post hoc log assembly.
- I gain temporal queries because I can answer what was known at a past timestamp, not only what is true now.
- I gain explicit optimistic concurrency control instead of accepting silent last-write-wins corruption.
- I gain correct schema evolution via upcasters instead of destructive historical migrations.
- I gain reliable downstream publication via the outbox pattern rather than a fragile "write here and publish there" split-brain workflow.

My conclusion is that callback traces in this project are EDA-style observability. The Ledger is Event Sourcing because the events themselves become the source of truth, the replay mechanism, and the system’s durable memory.

## 2. Aggregate Boundaries as Consistency Decisions

The four aggregate boundaries in the challenge are correct because they are framed as consistency boundaries and invariant ownership boundaries.

- `LoanApplication` owns the lifecycle of the commercial loan application and the invariants around state transitions, approval preconditions, and credit-limit constraints.
- `AgentSession` owns the integrity of a single AI agent work session, including context declaration, model-version accountability, reasoning trace linkage, and session-scoped outputs.
- `ComplianceRecord` owns regulatory checks, rule-evaluation evidence, mandatory-check completeness, and regulation-version traceability.
- `AuditLedger` owns cross-stream audit correlation and append-only integrity chains across related business entities.

The alternative boundary I considered and rejected was collapsing `ComplianceRecord` into `LoanApplication` so that all application-related business facts would be written to the `loan-{application_id}` stream.

I rejected that design because it would couple distinct consistency concerns into one unnecessarily hot stream. The correct question in aggregate design is not "what objects are related?" but "what facts must be transactionally consistent within one stream?" Compliance work is rule-by-rule, regulation-versioned, and often progresses on a different cadence from core application lifecycle decisions. If every `ComplianceRulePassed` and `ComplianceRuleFailed` event were appended to the loan stream, then compliance processing would contend directly with loan-state transitions, human-review events, and orchestrator decisions, even when they were not trying to change the same invariant.

That is a consistency-boundary mistake because it converts independent work into false write conflicts. A concrete failure scenario makes this clear. Suppose the compliance engine is appending the final mandatory `ComplianceRulePassed` event at the same moment a human loan officer appends `HumanReviewCompleted`. If both operations write to the same `LoanApplication` stream, one will lose the optimistic concurrency race, even though one operation concerns regulatory evidence completeness and the other concerns the post-decision human workflow. The result is unnecessary coordination pressure created by an oversized aggregate, not a real business contradiction.

Separating `ComplianceRecord` prevents that coupling. Compliance can evolve in its own stream until it reaches a verdict. `LoanApplication` then consumes the compliance facts at the decision point and enforces the relevant cross-aggregate invariant: an `ApplicationApproved` event must not be appended unless the compliance record proves that all mandatory checks have been satisfied. This preserves correctness without serializing unrelated write paths through one stream.

The same principle justifies keeping `AgentSession` separate from `LoanApplication`. If session-level facts such as `AgentContextLoaded`, tool invocations, model-version declarations, and reasoning outputs were merged into the loan stream, the application stream would become a mixed responsibility stream that combines lifecycle facts with execution telemetry. That would enlarge the contention surface, weaken aggregate clarity, and create false conflicts between agent-execution details and business-state transitions. By isolating `AgentSession`, I preserve the Gas Town invariant locally: no decision event may exist without a prior context-loading event, and every material agent output remains tied to a specific model version and session.

My design judgment is therefore that the chosen aggregates are correct because they minimize false contention, keep invariants local to the stream that owns them, and force cross-aggregate coordination to happen through durable events rather than shared mutable state.

## 3. Concurrency Control in Practice

The scenario is that two AI agents simultaneously process the same loan application and both call `append_events` against `loan-{application_id}` with `expected_version = 3`. The exact sequence matters because the rubric is assessing whether I understand runtime conflict mechanics, not merely the slogan "reload and retry."

The correct sequence is as follows.

1. Both agents load `loan-{application_id}` when the stream is at `stream_position = 3`.
2. Each agent replays the first three stored events to reconstruct the same in-memory aggregate state.
3. Each command handler determines a new event or event batch based on that reconstructed state.
4. Agent A begins a database transaction and attempts the append.
5. Inside the transaction, the event store enforces optimistic concurrency at the database level. This must be implemented through SQL semantics, not through an application-level `if current_version == expected_version` check in Python. In practice, this usually means a guarded update of `event_streams.current_version`, or an equivalent compare-and-swap pattern, plus the append of the new `events` rows and corresponding `outbox` rows in the same transaction.
6. Because the actual stream version is still 3, Agent A succeeds. The store writes the new event row or rows, assigns `stream_position = 4` to the first new event, advances `event_streams.current_version` to 4, writes the outbox payloads transactionally, and commits.
7. Agent B then reaches the same guarded append path while still asserting `expected_version = 3`.
8. The database now observes that the actual version is 4, not 3. The compare-and-swap condition fails, or the transactional write path detects that the expected version no longer matches the authoritative stream version.
9. Agent B’s transaction is rolled back.
10. Agent B receives a structured `OptimisticConcurrencyError` containing, at minimum, the `stream_id`, `expected_version = 3`, `actual_version = 4`, and a machine-actionable suggestion such as `reload_stream_and_retry`.

What the losing agent must do next is equally important. It must not simply replay the same SQL write. It must reload the stream, reconstruct the aggregate from the new authoritative event history, and then re-run the domain decision. That step exists because the newly appended event may have changed the business validity of the losing command. In this domain, the losing agent may now discover that a credit analysis has already been recorded, that the decision has advanced to a later lifecycle state, or that a model-version-locking invariant forbids a second write of the same class. The retry unit is therefore the command workflow, not the database statement.

This is why optimistic concurrency control is the correct enterprise pattern here. It does not pretend conflicts do not happen. It makes them explicit, typed, and recoverable.

## 4. Projection Lag and the System Response

The prompt states that the `LoanApplication` projection has a typical lag of 200 milliseconds and a loan officer queries "available credit limit" immediately after a disbursement event has committed. They see the old limit. The correct interpretation is not that the write failed. The correct interpretation is that CQRS is behaving normally: the command side is current, while the read side is temporarily stale.

The first thing the system does is commit the command on the write side and return an acknowledgement that reflects authoritative commit success. That acknowledgement should include the new stream version and, ideally, the committed `global_position`, because those fields provide a freshness watermark the caller can use to reason about projection completeness.

The second thing the system does is refuse to treat the projection as stronger than the event store. The query side remains eventually consistent by design. I would therefore not "fix" the stale read by consulting the projection during command handling or by weakening the CQRS boundary. Instead, I would make freshness explicit in the application contract.

My operational behavior would be:

1. The disbursement command returns success, the new aggregate version, and the committed global position.
2. The user interface marks the post-command state as pending projection confirmation.
3. The UI either polls or subscribes until the relevant projection checkpoint has advanced beyond the returned global position.
4. Until that freshness condition is met, the UI displays the available credit value as "updating" rather than as settled truth.
5. Any follow-on action that depends on the new credit limit should be disabled or guarded until the projection catches up.

The communication to the user must also be explicit. I would not show the old value without qualification. I would show a message such as: "The disbursement has been recorded in the ledger. The application summary is updating and may reflect the previous limit for a few hundred milliseconds." That wording matters because it distinguishes command durability from projection freshness.

This is also why lag measurement is a non-negotiable metric in the challenge documents. The system is expected to expose projection lag, compare it to service-level objectives, and let operators and UIs reason about stale-read windows. The system response is therefore not merely "the projection is stale." The system response is: acknowledge the write from the authoritative log, surface freshness metadata, let the query model converge, and communicate that short convergence window honestly to the caller.

## 5. Upcasting with Field-Level Inference Reasoning

The rubric states the scenario in terms of a hypothetical `CreditDecisionMade` event that evolved between 2024 and 2026. I am addressing that scenario directly because it tests my understanding of upcasting mechanics and field-level inference. At the same time, I note that this specific codebase currently points to two concrete upcaster targets in `src/upcasting/registry.py` and `src/upcasting/upcasters.py` and the challenge document: `CreditAnalysisCompleted v1 → v2` and `DecisionGenerated v1 → v2`. The same upcasting principles apply in both cases: upcasters run on read, preserve immutability of the stored event, and upgrade older payloads into the current schema expected by the domain layer.

The rubric scenario is that `CreditDecisionMade` was stored in 2024 with the payload:

- `application_id`
- `decision`
- `reason`

In 2026, the event schema requires:

- `application_id`
- `decision`
- `reason`
- `model_version`
- `confidence_score`
- `regulatory_basis`

In an event-sourced system, I must not mutate the stored 2024 events. Instead, I implement a deterministic read-time upcaster. The structurally correct shape, aligned with the challenge material, is a versioned upcaster registered in an `UpcasterRegistry`.

```python
@registry.register("CreditDecisionMade", from_version=1)
def upcast_credit_decision_v1_to_v2(payload: dict, recorded_at: str | None = None) -> dict:
    return {
        **payload,
        "model_version": infer_model_version(recorded_at),
        "confidence_score": None,
        "regulatory_basis": infer_regulatory_basis(recorded_at),
    }
```

If the registry only supports payload-only functions, then the store should supply envelope fields such as `recorded_at` to the inference layer before applying the transformation, because the inference strategy depends on the event’s historical context. The important structural point is that the upcaster is deterministic, side-effect free, and applied at load time without touching the stored row.

My inference strategy is field-specific.

For `model_version`, I would use a deterministic legacy mapping based on deployment era if I have trustworthy deployment metadata or a policy timeline. For example, if all pre-2026 decisions were known to originate from a stable legacy model family, I would infer a sentinel value such as `"legacy-pre-2026"` or a documented legacy deployment identifier. I would not fabricate a precise version string that cannot be justified by historical evidence. The challenge documents explicitly warn that some missing fields should be inferred while others should remain null. For model version, a coarse but honest legacy label is preferable to false precision.

For `confidence_score`, I would set `None`. This is the clearest example of a field that is genuinely unknowable for historical events if it was never captured. A synthetic confidence score would create fabricated evidence in a regulatory domain, which is worse than a missing value. In this case, null is the correct event-sourcing answer because it preserves historical truth.

For `regulatory_basis`, I would infer from a deterministic regulation calendar keyed by `recorded_at`. If the event timestamp falls inside a known rule regime, then the upcaster can map the event to the regulation set in force on that date. This is a valid inference because the missing value is not a subjective model output; it is a historical policy fact that can be reconstructed from a stable timeline. If the timestamp is missing or the regulatory calendar is ambiguous, I would fall back to a sentinel such as `"legacy-regulatory-basis-unknown"` rather than inventing a basis.

My governing rule is therefore this: infer only when the inference is deterministic, reproducible, and auditable. Use null or a documented legacy sentinel when the historical fact cannot be known with defensible confidence. That is the difference between a legitimate upcast and a historical rewrite.

Applied back to this project’s concrete event catalogue, my interpretation is as follows.

- For `CreditAnalysisCompleted v1 → v2`, the correct upgrade path is not merely "add any missing field." It is to add the new semantic fields required by the evolved event contract, such as `model_version`, `confidence_score`, and `regulatory_basis`, with field-by-field reasoning about which values are inferable and which must remain null or legacy-marked.
- For `DecisionGenerated v1 → v2`, adding `model_versions` is appropriate because the challenge materials explicitly tie the orchestrator decision to contributing agent sessions. The defensible strategy is to reconstruct that map from the referenced `AgentSession` streams, specifically from their `AgentContextLoaded` or equivalent model-declaration events. That inference is more expensive because it requires store lookups, but it is still legitimate if it is deterministic and performed on read rather than by mutating the event store.

This means the project’s upcasting design must satisfy two standards simultaneously: it must be structurally correct for the current codebase’s target events, and it must demonstrate the deeper event-sourcing judgment required by the rubric, namely when to infer, when to emit null, and when to preserve an explicit legacy sentinel rather than fabricate history.

## 6. Distributed Projection Coordination and the Failure Mode It Must Prevent

To achieve the Marten Async Daemon pattern in Python, I would run multiple projection-daemon workers but coordinate ownership of projection partitions through PostgreSQL advisory locks combined with durable projection checkpoints.

The pattern is straightforward.

1. Each daemon instance competes to acquire ownership of a projection partition, for example by `projection_name` or by `projection_name + shard_id`.
2. Ownership is granted through a PostgreSQL advisory lock such as `pg_try_advisory_lock(lock_key)`.
3. Only the node that holds the advisory lock may read forward from the relevant checkpoint, apply projection handlers, and advance `projection_checkpoints`.
4. If the node crashes or loses its database session, PostgreSQL releases the advisory lock automatically.
5. A surviving node can then acquire the lock and resume from the last committed checkpoint.

The specific failure mode this coordination primitive guards against is split-brain projection ownership. Without coordination, two daemon nodes can process the same event range for the same projection at the same time. That creates several concrete hazards.

- Both workers may attempt to mutate the same projection rows concurrently.
- Both workers may race to advance the same checkpoint, causing checkpoint drift or lost progress accounting.
- Both workers may emit duplicate downstream side effects if any projection handler is not perfectly idempotent.
- Operators may observe inconsistent lag metrics because more than one node believes it is the legitimate owner of the same projection shard.

That is the primary failure mode I must guard against, and it is exactly the sort of enterprise coordination bug that Marten’s distributed daemon exists to solve.

The advisory lock is a good primitive here because it is colocated with the PostgreSQL event store, requires no separate coordinator, and is automatically released when the owning session dies. I would still combine it with careful checkpoint discipline: a daemon must advance `projection_checkpoints` only after a batch has been applied successfully. If a daemon crashes after partially updating projection tables but before checkpoint commit, the replacement worker will replay the batch from the previous checkpoint. That replay is acceptable only if the handlers are idempotent, which is why idempotency is a required property of production projection logic.

My Python equivalent of the Marten Async Daemon is therefore: background asyncio workers, PostgreSQL advisory locks for exclusive projection ownership, checkpoint-based recovery, idempotent projection handlers, and lag metrics derived from `global_position - last_processed_position`. This pattern prevents split-brain processing, limits recovery ambiguity, and preserves the integrity of eventually consistent read models across multiple nodes.

## Closing Position

My overall position is that The Ledger must be designed as a genuine event-sourced system rather than as a tracing-enhanced CRUD application. That means I must distinguish EDA from Event Sourcing with precision, treat aggregate boundaries as consistency decisions, implement optimistic concurrency as a database-enforced compare-and-swap discipline, handle stale projections with explicit freshness communication, upcast old events without mutating history, and coordinate distributed projection workers to prevent split-brain ownership. In a regulated AI lending domain, these are not optional refinements. They are the minimum architectural conditions for auditability, reproducibility, and operational trustworthiness.
