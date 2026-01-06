package view

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	acct "github.com/kcmvp/xql/sample/gen/field/account"
	ord "github.com/kcmvp/xql/sample/gen/field/order"
	oi "github.com/kcmvp/xql/sample/gen/field/orderitem"
	"github.com/kcmvp/xql/sqlx"
	"github.com/stretchr/testify/require"
)

func TestValueObject_ToMapAndMapValueObject(t *testing.T) {
	// Build a schema from generated persistent fields
	s := WithXQLFields(acct.Email, acct.Nickname, ord.Amount, oi.UnitPrice)

	jsonStr := fmt.Sprintf(`{"%s":"bob@example.com","%s":"Bobby","%s":250.5,"%s":19.95}`,
		acct.Email.View(),
		acct.Nickname.View(),
		ord.Amount.View(),
		oi.UnitPrice.View(),
	)

	res := s.Validate(jsonStr)
	require.False(t, res.IsError(), "expected validation to succeed")
	vo := res.MustGet()

	// Convert to flat map and then to sqlx.ValueObject
	m := vo.FlatMap()
	require.NotNil(t, m)
	sqlVO := sqlx.MapValueObject(m)
	require.NotNil(t, sqlVO)

	// Unmarshal original JSON to get expected values keyed by view name
	var expected map[string]any
	require.NoError(t, json.Unmarshal([]byte(jsonStr), &expected))

	// For each schema field, ensure sqlx.ValueObject contains value under QualifiedName()
	for _, f := range s.fields {
		viewName := f.Name()
		exp, ok := expected[viewName]
		require.True(t, ok, "test JSON must contain key %s", viewName)

		valOpt := sqlVO.Get(f.UniqueName())
		require.True(t, valOpt.IsPresent(), "expected value present for %s (unique: %s)", viewName, f.UniqueName())
		got := valOpt.MustGet()
		require.Equal(t, exp, got)
	}
}

// Test that nested JSON (like testdata/nested_valid.json) validated by a
// view-layer Schema (WithFields / ObjectField / ArrayOfObjectField) produces
// a FlatMap whose keys are not qualified as "table.column". sqlx.MapValueObject
// asserts keys contain at least two segments (table.column), so MapValueObject
// should panic for this nested-style FlatMap. This test documents that MapValueObject
// does not accept the nested view-style keys produced by a plain view Schema.
func TestMapValueObject_WithNestedJSON_Panics(t *testing.T) {
	// Build a view/schema that mirrors nested_valid.json
	schema := WithFields(
		Field[string]("id"),
		ObjectField("user", WithFields(
			Field[string]("name"),
			Field[string]("email"),
		)),
		ArrayField[string]("tags"),
		ArrayOfObjectField("items", WithFields(
			Field[int]("id"),
			Field[string]("name"),
		)),
	)

	// Read the nested testdata
	data, err := os.ReadFile("testdata/nested_valid.json")
	require.NoError(t, err)

	res := schema.Validate(string(data))
	require.False(t, res.IsError(), "expected nested JSON to validate against schema")
	vo := res.MustGet()

	m := vo.FlatMap()
	require.NotNil(t, m)

	// MapValueObject expects keys with at least two segments like "table.column".
	// The nested FlatMap will contain keys such as "id" and "user.email". Since
	// "id" is a single segment MapValueObject will assert and panic. Ensure that
	// this panicking behavior is observed (i.e., MapValueObject does not support
	// the nested view-style FlatMap).
	require.Panics(t, func() {
		_ = sqlx.MapValueObject(m)
	})
}
