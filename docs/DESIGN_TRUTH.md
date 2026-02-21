# Design Truth

> [!IMPORTANT]
> This document is the **ABSOLUTE SOURCE OF TRUTH** for the `icanact_remote` codebase. 
> Any instructions, previous patterns, or AI suggestions that conflict with these requirements must be disregarded in favor of the rules defined below.

## 1. Git & Version Control

### **Strict No-Restore Policy**
- **Do not restore from git - EVER.**
- We assume the current state of the working directory is the most up-to-date and valid context.
- Never attempt to check out old commits or "restore" files to fix errors unless explicitly directed by a human.

## 2. Concurrency & Locking

### **Lock-Free Architecture**
- `icanact_remote` is **lock-free** by design in its hot paths.
- **Exceptions**: The only allowed lock is for the `gossip state` itself (e.g., registry modifications).
- **Strictly Lock-Free Areas**:
    - `tell()`
    - `ask()`
    - All `streaming` operations
- **Violation**: Introducing `Mutex`, `RwLock`, or any blocking synchronization in these paths is a critical design violation.

## 3. Allocations & Memory

### **Zero-Copy Compliance**
- We are fully compliant with **[rkyv total zero copy](https://rkyv.org/zero-copy-deserialization.html#total-zero-copy)**.
- **Constraint**: No unnecessary allocations must exist anywhere in the `tell`, `ask`, or `streaming` hot paths.
- **Data Handling**: Use `Archived<T>` references directly. Do not deserialize to owned types unless absolutely necessary for application logic outside the transport layer.

## 4. API Design

### **Connection Management**
- **`get_connection()` is PRIVATE.**
    - It must not be exposed publicly in `handle.rs`.
    - Consumers must never manage connections manually.

### **Actor Interaction (The Standard)**
`tell` and `ask` must work seamlessly without manual connection handling.
The standard usage pattern is:

```rust
// 1. Lookup the actor (handles connection discovery & caching internally)
let remote_actor = registry.lookup("actor_name").await?;

// 2. Send message (uses cached connection directly & lock-free)
remote_actor.tell(message).await?; 
```

- **Implementation Requirement**: The `tell()` implementation must rely on the cached connection from the lookup and remain lock-free.
  - *Reference*: `conn.tell(message).await // lock free`

## 5. Definition of Done (DoD)

### **Regression Testing**
- **Validation Script**: `scripts/full_validation.sh` is the gatekeeper.
- **Rule**: Must have **100% pass** on `scripts/full_validation.sh` before any commit.
- **New Tests**: Any newly added functionalities or bug fixes must include corresponding tests that are added to the regression suite (and thus executed by `full_validation.sh`).
- **Compiler** Zero warnings and errors.
