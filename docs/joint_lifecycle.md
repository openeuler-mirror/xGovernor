# Joint execution lifecycle

`RuntimeRegistration::new` retains the provider/runtime split lifecycle. An explicit `with_joint_lifecycle` strategy can instead own indivisible execution resources. This is an internal application boundary; it adds no HTTP fields and no server backend registration.

The application retains authorization, environment normalization, capability checks, leases, public identifiers and event projection. `JointOperationContext` carries the authenticated role/quota, tenant ownership, parent intent, public identities and pinned configuration revision. It never carries model credentials.

A joint owner returns records already committed by the supplied durable repository. The application checks their identity and persisted result; it does not independently save a second copy or adjust resource references. Ordinary SQLite session writes cannot create a joint record or replace an existing joint/unknown route with a split route.

Session routing is persisted as a versioned `SessionExecutionBacking`. Existing databases acquire the `split_v1` default without changing their records. Unknown variants fail decoding. Joint bindings name an owner, binding and configuration revision; they are internal and excluded from public projections.

Open, capture, load, close, attachment and health checks use the joint strategy for joint records. Unsupported turns, interaction, cancel, fork and Host operations fail before invoking a split runtime/provider. Closing a joint session invokes one owner and never the split cleanup sequence. Joint operations do not hold the legacy global snapshot lock across downstream work; they require durable source/Root fencing.

`SessionRepository::supports_joint_transactions` defaults to false. The SQLite repository requires an explicit `with_joint_transactions()` assembly opt-in for a verified joint owner; its default remains disabled. The legacy path remains usable. A strategy must not claim this capability by relying on default no-op intent methods.

Joint checkpoint records use a real binding reference; legacy checkpoints keep their real provider snapshot and runtime state. No placeholder provider snapshot ID is used to represent a joint resource. Durable migration/publication and recovery are requirements for enabling a joint owner, and do not follow merely from selecting the strategy.

## Durable admission and publication

SQLite stores joint sessions and checkpoints in independent tables in the same database. Legacy readers see only split records; triggers prevent a legacy writer from taking a published or reserved joint public identity. New readers merge the two routes for authorized listings and quota restoration. Ordinary `save` remains exclusively for split records.

The durable repository API commits a binding, public identity claim, native UUID/digest/parameters, reservations, parent reference and Root/source fence before downstream effects. A repeated parent intent must match the original request exactly. Admission counts active legacy sessions plus joint reservations, including unpublished and unknown operations. A new Root requires its own reservation. Every later binding in that Root has an explicit active parent with matching tenant, owner, runtime and configuration revision. Independent native objects have one owner within a Root.

Record native outcomes with their exact original UUID and digest. Accepted, unknown and failed results retain the union of resource identities already observed for the same native step. The payload is the latest observation; intermediate payload history is not retained. Terminal outcomes remain immutable. An unknown result does not release a reservation, identity claim or fence. A later create result invalidates earlier absence for its objects. Native parameters and results must be sanitized by the trusted adapter before persistence; the repository rejects known credential fields but does not provide arbitrary secret discovery.

Public session/checkpoint publication atomically checks parent stage, binding generation, Root/source fence and completed native results, then commits the public record, reference, reservations and publication result. Replaying the same publication returns the committed result. No provider snapshot placeholder or separate second save is involved.

A release first prevents new dependent references. Final release requires settled original operations, exact absence for every owned object in the binding generation and no other references. Unpublished creation has a separate compensation finish with the same requirements; unknown delivery cannot be treated as not sent. Known resource identities, UUIDs and terminal outcomes remain after release for reconciliation. Pending intents are dispatched to their persisted joint owner; the application does not create replacement mutations or invoke legacy cleanup.

This API supplies durable storage primitives. `supports_joint_transactions` remains false by default; an explicitly assembled owner can opt into the verified SQLite implementation. These primitives do not enable a server backend or provide a multi-writer deployment contract for the legacy lifecycle.

A prepared operation can atomically enter compensation after all original mutations
are settled and no public/dependent reference exists. Compensation is a durable
phase: ordinary sends and publication are excluded, including after restart before
the first cleanup step. Native steps identify their execution or compensation
purpose; new optional fields preserve the encoding of legacy intents. Safe Session
publication input can be persisted with admission and cannot be replaced later.

## Assembled resource budgets

Configure `SqliteSessionRepository::with_joint_budget_policy` on every repository
that admits joint resources. `JointBudgetPolicy` maps a resource kind to a
`JointResourceBudget { total, recovery_reserve }`; `recovery_reserve` defaults to
zero. An omitted kind has zero capacity, and an empty policy denies new
allocations. Assembly rejects a reserve larger than its total or an unknown
resource kind. Setting a policy does not enable `supports_joint_transactions`;
the separate lifecycle opt-in remains necessary.

The policy is supplied by trusted server assembly, not by an operation request.
It is not persisted or revisioned in SQLite. All processes accessing one database
must be assembled with the same policy, and coordinated configuration changes
must update every writer. This deployment requirement does not introduce a
multi-writer contract for the legacy lifecycle. Reopening with lower limits
retains existing records and reservations, and rejects new ordinary allocations
while the remaining capacity is insufficient. Recovery, exact request replay,
publication and release of an admitted operation do not allocate again and are
not blocked by a later policy reduction or an empty policy.

All supported allocations (`open`, `load`, `checkpoint`) are ordinary. For each
kind they must satisfy both `ordinary_used + units <= total - recovery_reserve`
and `total_used + units <= total` in the same prepare transaction, before native
execution. Every unreleased reservation contributes to both counts; for sessions,
active legacy sessions contribute too. An authenticated tenant's `max_sessions`
further limits session admission. Each tenant/owner is accounted independently.
Session and Root reserves are zero unless explicitly configured otherwise.

`JointResourceReservation.limit` is accepted only for decoding older requests
and source compatibility. Its value is ignored for admission and replay identity,
and new serialized requests omit it. Older stored request bodies remain intact
and can be decoded, reconciled and published. Other request fields, including
native UUIDs, digests, parameters and resource quantities, still have to match on
replay. The legacy Session/Root limit arguments to `RootSessionOwner::new` are
also accepted for source compatibility and do not control admission; repository
assembly is the sole resource-budget authority.

Reserved and committed units both remain charged. Unknown delivery,
`needs_recovery`, known failure with residual objects and pending cleanup do not
release capacity through timeout, TTL, restart or a single missing projection.
Release requires confirmed non-delivery without residual resources, or settled
original operations, validated absence of all owned objects and no surviving
references. Release is transactional and idempotent; repeating it cannot return
capacity twice.

A nonzero recovery reserve withholds capacity from ordinary admissions; it does
not create resources or provide a repair facility. The Root/direct owner consumes
only ordinary Session/Root capacity. No supported operation consumes the reserved
recovery pool. Replica creation, failure budgets, minimum retained copies,
replacement publication and retirement of old copies require their own resource
lifecycle implementations. Reservation names such as `replica` and `repair` do
not establish those capabilities, and capacity pressure never authorizes deletion
of a last usable copy.


Concurrent open recovery accepts a committed Session only after matching the
original intent, binding, generation, complete creation request and publication
input. The idempotent publication transaction verifies the full admission token
and persisted result. A public runtime ID alone is insufficient. If another
executor publishes while a caller is resuming a pending operation, that caller
returns the verified committed result without another native mutation. Missing or
mismatching durable evidence remains an error.

Unpublished Root compensation resolves residual identities from the durable
binding, including older journals whose last outcome omitted a previously known
Root. A failed create with no known object in either the outcome or binding keeps
its ownership and capacity; an empty result is not proof of absence.

The internal binding lookup keeps live objects in `objects` and exposes known
objects with durable absence evidence separately in `absent_objects`, scoped to
the same binding generation. After compensation reaches durable absence, a
restart can complete the existing strict finish transaction without repeating
stop/remove, including older journals whose create result omitted the identity.
A failed create with no identity in either object collection or its outcome is
still rejected, even if its operation is already compensating. Release still
requires settled native steps, no surviving references and no live objects.

Repeated terminal observations first retain the previously known resource
identities, then compare the normalized result. The same state and payload with
no new resource identity is idempotent, including an empty incoming resource list.
A changed terminal state, payload or added resource identity remains a conflict.
