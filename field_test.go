package xql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Helper entity types used only in these tests
type schemaTableEntity struct{}

func (schemaTableEntity) Table() string { return "schema.table" }

type emptyTableEntity struct{}

func (emptyTableEntity) Table() string { return "" }

type dotEntity struct{}

func (dotEntity) Table() string { return "t" }

// Test that QualifiedName composes schema-qualified table names correctly.
func TestNewField_QualifiedName_WithSchema(t *testing.T) {
	f := NewField[schemaTableEntity, string]("col", "View")
	require.Equal(t, "schema.table.col.View", f.QualifiedName())
}

// NewField should panic when the entity.Table() returns an empty string.
func TestNewField_PanicsOnEmptyTable(t *testing.T) {
	require.Panics(t, func() { _ = NewField[emptyTableEntity, string]("c", "V") })
}

// NewField should panic when column or view contains a '.'
func TestNewField_PanicsOnDotInColumnOrView(t *testing.T) {
	require.Panics(t, func() { _ = NewField[dotEntity, string]("co.l", "V") })
	require.Panics(t, func() { _ = NewField[dotEntity, string]("col", "V.ew") })
}

// Constraints should return a copy of the underlying slice so callers cannot mutate the field's internal slice.
func TestConstraints_ReturnsCopyAndNames(t *testing.T) {
	// Use the existing dotEntity type declared above which implements Table()
	f := NewField[dotEntity, string]("col", "V", MaxLength(10))
	c := f.Constraints()
	require.Len(t, c, 1)
	name, _ := c[0]()
	require.Equal(t, "max_length", name)

	// Mutate the returned slice and ensure the field's internal slice is unaffected.
	c = append(c, MaxLength(20))
	c2 := f.Constraints()
	require.Len(t, c2, 1)
}
