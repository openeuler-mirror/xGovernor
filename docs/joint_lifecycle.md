# Joint execution lifecycle

`RuntimeRegistration::new` retains the provider/runtime split lifecycle. An explicit `with_joint_lifecycle` strategy can instead own indivisible execution resources. This is an internal application boundary; it adds no HTTP fields and no server backend registration.

The application retains authorization, environment normalization, capability checks, leases, public identifiers and event projection. `JointOperationContext` carries the authenticated role/quota, tenant ownership, parent intent, public identities and pinned configuration revision. It never carries model credentials.

A joint owner returns records already committed by the supplied durable repository. The application checks their identity and persisted result; it does not independently save a second copy or adjust resource references. Ordinary SQLite session writes cannot create a joint record or replace an existing joint/unknown route with a split route.

Session routing is persisted as a versioned `SessionExecutionBacking`. Existing databases acquire the `split_v1` default without changing their records. Unknown variants fail decoding. Joint bindings name an owner, binding and configuration revision; they are internal and excluded from public projections.

Open, capture, load, close, attachment and health checks use the joint strategy for joint records. Unsupported turns, interaction, cancel, fork and Host operations fail before invoking a split runtime/provider. Closing a joint session invokes one owner and never the split cleanup sequence. Joint operations do not hold the legacy global snapshot lock across downstream work; they require durable source/Root fencing.

`SessionRepository::supports_joint_transactions` defaults to false. The standard SQLite repository does not yet enable this capability, so the joint strategy remains unavailable until transactional ownership, fencing and publication are provided. The legacy path remains usable. A strategy must not claim this capability by relying on default no-op intent methods.

Joint checkpoint records use a real binding reference; legacy checkpoints keep their real provider snapshot and runtime state. No placeholder provider snapshot ID is used to represent a joint resource. Durable migration/publication and recovery are requirements for enabling a joint owner, and do not follow merely from selecting the strategy.
