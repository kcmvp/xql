package xql

import (
	"fmt"
	"strings"
	"time"

	"github.com/kcmvp/xql/entity"
	"github.com/samber/lo"
)

// Package dvo provides a compact, stable representation of entity field
// metadata used by code generation, SQL builders and view/JSON validation.
//
// Key goals:
//  - Keep the metadata model small and deterministic so generated Schemas
//    are easy to inspect and consume at runtime.
//  - Expose a small read-only API for field descriptors used by generators
//    and runtime adapters.
//  - Provide a tiny factory helper `NewField` that derives the DB table
//    name from a concrete entity type (via `entity.Entity`) and validates
//    basic invariants early.
//
// Typical usage (generator-emitted code):
//
//   var ID = NewField[Account, int64]("id", "ID")
//   // Schema values are slices of Field produced by generator code.
//
// Notes on layering:
//  - Validator factories (`ValidateFunc`) may be attached to fields by the
//    generator for convenience. In strict layering designs validators can
//    be owned by the `view` package at runtime.

// sealer is a private token type used to seal implementations of the Field interface.
// Requiring this unexported type in the unexported seal method prevents other
// packages from implementing Field because they cannot name the parameter type.
// Keeping the type in this package gives the strongest enforcement.
type sealer struct{}

// Field describes a single field's persistence metadata.
//
// Implementations provide read-only accessors:
//   - QualifiedName(): the DB-qualified column name in the form "table.column[.view]".
//     For persistent fields we encode the view as a third segment: "table.column.view".
//   - Scope(): the field's scope (table).
//   - ViewName(): the view/json key associated with the field (the last segment).
//
// Implementations may be provided by this module or by adapters in other
// packages (view wrappers, generator output, etc.).
type Field interface {
	// Scope returns the field's scope; it returns the table name for a persistent field.
	Scope() string
	// QualifiedName returns a DB-qualified column reference. For generated
	// persistent fields we return the composite "table.column.view" which
	// encodes the view name as the last segment.
	QualifiedName() string
	// View returns the view/json key (the last segment) for this field.
	View() string
	// seal prevents external packages from implementing Field by requiring the
	// unexported `sealer` parameter type which cannot be named outside this package.
	seal(sealer)
}

// Number is a type constraint for numeric native Go types.
type Number interface {
	uint | uint8 | uint16 | uint32 | uint64 |
		int | int8 | int16 | int32 | int64 |
		float32 | float64
}

// FieldType is a constraint for the concrete Go types that fields may
// carry as type hints for validators and code generation.
type FieldType interface {
	Number | string | time.Time | bool
}

// PersistentField is the internal, immutable implementation of Field.
// Instances are produced using `NewField`.
type PersistentField[E FieldType] struct {
	table  string
	column string
	view   string
	vfs    []ValidateFunc[E]
}

func (f *PersistentField[E]) Scope() string {
	return f.table
}

// QualifiedName returns the DB-qualified identifier. For persistent fields
// we include the view as the last segment: "table.column.view". The table
// component may itself contain '.' (schema-qualified table names are
// supported). We ensure the returned string is stable and deterministic.
func (f *PersistentField[E]) QualifiedName() string {
	// Compose as table.column.view
	return fmt.Sprintf("%s.%s.%s", f.table, f.column, f.view)
}

// ViewName returns the view (JSON key) part of the persistent field.
func (f *PersistentField[E]) View() string {
	return f.view
}

// implement seal so PersistentField satisfies Field
func (f *PersistentField[E]) seal(sealer) {}

var _ Field = (*PersistentField[int64])(nil)

// Constraints returns a copy of the validator factory functions attached to the
// persistent field. Callers may convert these factories into concrete
// validators suitable for their layer.
func (f *PersistentField[E]) Constraints() []ValidateFunc[E] {
	if f == nil || len(f.vfs) == 0 {
		return nil
	}
	cp := make([]ValidateFunc[E], len(f.vfs))
	copy(cp, f.vfs)
	return cp
}

// NewField creates a Field for entity type E with Go type hint T.
//
// Parameters:
//   - column: DB column name. Must be non-empty and contain no '.'.
//   - view: JSON/view key name (used as the provider/view name). Must be non-empty and contain no '.'.
//   - vfs: optional validator factory functions for the field.
//
// Behavior:
//   - The table name is derived by instantiating a zero value of E and
//     calling its Table() method. The function asserts the table and the
//     provided strings are non-empty using `lo.Assert`.
func NewField[E entity.Entity, T FieldType](column string, view string, vfs ...ValidateFunc[T]) *PersistentField[T] {
	var e E
	table := e.Table()
	lo.Assert(strings.TrimSpace(table) != "", "table must not return empty string")
	lo.Assert(column != "", "column must not return empty string")
	lo.Assert(view != "", "view must not return empty string")
	// column and view must not contain '.' to keep provider parsing simple
	if strings.Contains(column, ".") || strings.Contains(view, ".") {
		panic("column and view must not contain '.'")
	}
	return &PersistentField[T]{
		table:  table,
		column: column,
		view:   view,
		vfs:    vfs,
	}
}
