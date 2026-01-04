# Consolidated Plan: Refactor `view` to accept generated fields from `sample/gen`

## Purpose

This document describes a small-step, testable plan to make the view layer (`view.Schema`, `JSONField`) accept and reuse the generator-produced persistence `xql.Field` definitions found under `sample/gen/field/...`.

## Constraints

- Do not change any runtime code in this task unless you explicitly ask me to; this file is the plan only.  
- Keep each step as small and reversible as possible.  
- Prefer adding view-side adapters and generator-template changes rather than changing `xql` public APIs.

## High-level approach

1. Implement a small adapter inside the `view` package that converts an `xql.Field` (the generated persistent field) into a `view.ViewField` (the type `WithFields` accepts).
2. Unit-test the adapter in isolation.
3. Teach the generator to emit minimal typed view wrappers that call the adapter (one per entity, in a separate `_view_gen.go` file).
4. Regenerate sample outputs and run tests.
5. Replace one example or test with generated wrappers and iterate until stable.

## Why this approach

- Keeps `xql` stable and backwards-compatible.
- Localizes view-specific logic in `view`, making review and rollback trivial.
- Uses the generator to emit glue code so consumers can use generated fields directly in `WithFields`.

## Step-by-step plan (small steps)

### Step 0 — Review & branch (manual)

- Create a short-lived feature branch for the refactor.  
- Confirm current tests pass (baseline).

### Step 1 — Add a tiny adapter in `view` (very small)

- Create file: `view/persistent_adapter.go` (new).
- Purpose: expose `FromPersistent[T]` or `WrapField` that returns a `view.ViewField` implemented inside `view` and producing the appropriate `ViewField` when called.
- API sketch (informational, for implementers):
  - func WrapField[T validator.FieldType](f xql.Field, isArray, isObject bool, vfs ...validator.ValidateFunc[T]) ViewField
  - Behavior: convert validator factories to concrete `validator.Validator[T]`, set `required` true by default, allow `Optional()` to be called later on the returned `*JSONField[T]` if needed.
- Why small: This is a single new file implementing only view-side behavior, no generator changes yet.

### Step 2 — Unit-tests for adapter (very small)

- Create file: `view/persistent_adapter_test.go`.
- Tests (keep minimal):
  - `TestWrapField_Basics` — wrap an `xql` field and assert `Name()`, `Scope()`, and flags match what you passed.
  - `TestWrapField_Validators` — attach a simple validator (e.g., `validator.MaxLength`) and assert `validateRaw` fails/passes as expected.
- Run `go test ./view` and iterate until green.

### Step 3 — Update generator templates to emit view wrappers (small, localized template change)

- Prefer emitting a separate file per entity: `{{entity}}_view_gen.go` under the same generated package path (e.g. `sample/gen/field/order/order_view_gen.go`).
- Template change location: `cmd/gob/xql/resources/fields.tmpl` or add a new `view_fields.tmpl` used by the generator for emitting view wrappers.
- Emitted code (example snippet for a field):
  - `var ViewUnitPrice = view.WrapField[float64](UnitPrice, false, false, validator.Decimal[float64](10,2))`
- Notes for the generator: the generator must know the concrete Go type and whether the field is an array or embedded object (or alternatively emit `view.ObjectField` / `view.ArrayOfObjectField` calls for those cases).

### Step 4 — Regenerate sample `gen` files and update `testdata` if needed (small)

- Run the repository generator (the same action that `cmd/gob/xql/xql_generator_test.go` runs) to create the `_view_gen.go` files under `sample/gen/field/...`.
- If tests compare generated output to `testdata`, add a minimal `*_view_fields.json` or update existing `testdata` accordingly. Prefer keeping generator tests focused on persistence files; add new tests for view wrapper generation only if necessary.

### Step 5 — Integrate: replace one example/test with generated wrappers (small, reversible)

- Pick a single example/test (or create a small smoke test) and construct a `Schema` with `WithFields(ViewX, ViewY, ...)` using generated wrappers.
- Run `go test` for the affected packages. Fix small issues (imports, validator calls, typing) as they arise.

### Step 6 — Expand rollout gradually (incremental)

- After step 5 is robust, replace other manual `Schema` definitions with generated wrappers incrementally.
- Consider adding a generator flag to enable/disable view wrapper output.
- Add docs in README describing how to use generated view wrappers.

## Edge cases and clarifications

- Validator typing: generator must emit typed validator factories that match the concrete generics used by `WrapField[T]`. If a DB type implies `float64`, generator should emit `validator.Decimal[float64](p,s)`.
- Embedded objects & arrays: If generator can detect nested structs/slices, emit explicit calls to `view.ObjectField`/`view.ArrayOfObjectField` or pass `isObject/isArray` flags. Prefer explicit calls to reduce ambiguity.
- `xql.Field` visibility: adapter lives in `view`, so we do not need to make `PersistentField` or other types internal/public changes in `xql` during the first pass.

## Backwards compatibility and rollback strategy

- The adapter approach is non-invasive: nothing in `xql` or existing code needs to change.  
- If generator template changes cause issues, roll back templates; adapter code can be merged safely because it does not alter runtime behavior until generator emits wrappers and code references them.

## Files to create or change (summary)

- Add: `view/persistent_adapter.go` (new) — adapter.
- Add: `view/persistent_adapter_test.go` (new) — tests.
- Change: `cmd/gob/xql/resources/fields.tmpl` or add `view_fields.tmpl` — emit `_view_gen.go` files.
- Add (generated): `sample/gen/field/<entity>/<entity>_view_gen.go` (one per entity) — generated wrappers.
- Optional: `testdata/*_view_fields.json` if generator tests require it.

## Deliverables for this task

- A small adapter in `view` that exposes `WrapField[T]` (or `FromPersistent`) which returns a `ViewField`.
- Minimal unit tests proving the adapter works.
- Template diff to emit view wrappers (kept small).
- One example/test updated to use generated wrappers to prove end-to-end flow.

## Next action (proposed)

I will not change any runtime code now. If you approve this plan I can proceed to implement the very first small step: create `view/persistent_adapter.go` and `view/persistent_adapter_test.go`, run `go test ./view`, and iterate until tests pass.

Please confirm whether you want the generated view wrappers in separate files (`<entity>_view_gen.go`) or in the same file as the persistence fields (I recommend separate files).
