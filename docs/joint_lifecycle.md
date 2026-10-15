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

Record native outcomes with their exact original UUID and digest. Accepted, unknown and failed results retain any known partial objects. An unknown result does not release a reservation, identity claim or fence. A later create result invalidates earlier absence for its objects. Native parameters and results must be sanitized by the trusted adapter before persistence; the repository rejects known credential fields but does not provide arbitrary secret discovery.

Public session/checkpoint publication atomically checks parent stage, binding generation, Root/source fence and completed native results, then commits the public record, reference, reservations and publication result. Replaying the same publication returns the committed result. No provider snapshot placeholder or separate second save is involved.

A release first prevents new dependent references. Final release requires settled original operations, exact absence for every owned object in the binding generation and no other references. Unpublished creation has a separate compensation finish with the same requirements; unknown delivery cannot be treated as not sent. Partial results, UUIDs and failure history remain after release for reconciliation. Pending intents are dispatched to their persisted joint owner; the application does not create replacement mutations or invoke legacy cleanup.

This API supplies durable storage primitives. `supports_joint_transactions` remains false by default; an explicitly assembled owner can opt into the verified SQLite implementation. These primitives do not enable a server backend or provide a multi-writer deployment contract for the legacy lifecycle.

A prepared operation can atomically enter compensation after all original mutations
are settled and no public/dependent reference exists. Compensation is a durable
phase: ordinary sends and publication are excluded, including after restart before
the first cleanup step. Native steps identify their execution or compensation
purpose; new optional fields preserve the encoding of legacy intents. Safe Session
publication input can be persisted with admission and cannot be replaced later.
