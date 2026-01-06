package view

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	acct "github.com/kcmvp/xql/sample/gen/field/account"
	ord "github.com/kcmvp/xql/sample/gen/field/order"
	oi "github.com/kcmvp/xql/sample/gen/field/orderitem"
	prof "github.com/kcmvp/xql/sample/gen/field/profile"
	"github.com/kcmvp/xql/validator"
	"github.com/stretchr/testify/require"
)

func TestWrapFieldAndWithXQLFields_Basics(t *testing.T) {
	// Choose a few generated persistent fields and verify persistentField behavior
	fEmail := acct.Email
	vfEmail := PersistentField[string](fEmail)
	require.Equal(t, fEmail.QualifiedName(), vfEmail.QualifiedName())
	require.Equal(t, fEmail.Scope(), vfEmail.Scope())
	require.Equal(t, "Email", vfEmail.Name())

	// Nickname has a max length validator in the generated field.
	nick := acct.Nickname
	vNick := PersistentField[string](nick)
	long := strings.Repeat("a", 101)
	r := vNick.validateRaw(long)
	require.True(t, r.IsError(), "expected Nickname validator to reject long string")

	// Test WrapFieldAsArray / WrapFieldAsObject flags
	vfArray := PersistentField[string](fEmail).AsArray()
	require.True(t, vfArray.IsArray())
	vfObj := PersistentField[string](prof.Bio).AsObject()
	require.True(t, vfObj.IsObject())

	// Decimal constraint exists on orderitem.UnitPrice; ensure QualifiedName copies
	up := oi.UnitPrice
	vup := PersistentField[float64](up)
	require.Equal(t, up.QualifiedName(), vup.QualifiedName())

	// WithXQLFields should produce a Schema equivalent to WithFields(persistentField(...))
	name := PersistentField[string](acct.Email).Name()
	s1 := WithXQLFields(acct.Email)
	jsonStr := fmt.Sprintf(`{"%s":"john@example.com"}`, name)
	res := s1.Validate(jsonStr)
	require.False(t, res.IsError())

	s2 := WithFields(PersistentField[string](acct.Email))
	res2 := s2.Validate(jsonStr)
	require.False(t, res2.IsError())

	// Duplicate names should panic (two identical persistent fields)
	require.Panics(t, func() { WithXQLFields(acct.Email, acct.Email) })
}

func TestWithXQLFields_MultiFieldValidation(t *testing.T) {
	// Build a small schema composed of generated fields from different packages
	s := WithXQLFields(
		acct.Email,
		acct.Nickname,
		ord.Amount,
		oi.UnitPrice,
	)

	// Create JSON that includes the view names (last segment) and values.
	// Use values that should pass validation.
	jsonStr := fmt.Sprintf(`{"Email":"alice@example.com","Nickname":"Al","Amount":123.45,"UnitPrice":9.99}`)
	res := s.Validate(jsonStr)
	require.False(t, res.IsError(), "expected combined schema to validate")

	// Now make a Nickname too long which should fail
	badJSON := fmt.Sprintf(`{"Email":"alice@example.com","Nickname":"%s","Amount":123.45,"UnitPrice":9.99}`, strings.Repeat("x", 201))
	res2 := s.Validate(badJSON)
	require.True(t, res2.IsError(), "expected validation to fail on long nickname")
}

func TestWrapField_NameExtraction(t *testing.T) {
	f := ord.CreatedAt
	vf := PersistentField[time.Time](f)
	require.Equal(t, "CreatedAt", vf.Name())
}

func TestWrapField_DuplicateValidatorPanics(t *testing.T) {
	n := acct.Nickname
	require.Panics(t, func() {
		_ = PersistentField[string](n, validator.MaxLength(50))
	})
}

func TestWrapField_NilPanics(t *testing.T) {
	require.Panics(t, func() {
		_ = PersistentField[string](nil)
	})
}

func TestWrapField_MergesValidators(t *testing.T) {
	n := acct.Nickname
	vf := PersistentField[string](n, validator.MinLength(2))
	r := vf.validateRaw("A")
	require.True(t, r.IsError())
	r2 := vf.validateRaw("Abc")
	require.False(t, r2.IsError())
}

func TestWrapField_ArrayValidation(t *testing.T) {
	// Wrap Quantity as array and validate using WithFields
	q := PersistentField[int64](oi.Quantity).AsArray()
	s := WithFields(q)
	jsonStr := fmt.Sprintf(`{"%s":[1,2,3]}`, q.Name())
	res := s.Validate(jsonStr)
	require.False(t, res.IsError())
	// Non-array should fail
	bad := fmt.Sprintf(`{"%s": 123}`, q.Name())
	res2 := s.Validate(bad)
	require.True(t, res2.IsError())
}

func TestValueObject_BackendQualifiedKeys_HappyPath(t *testing.T) {
	s := WithXQLFields(acct.Email, acct.Nickname, ord.Amount, oi.UnitPrice)

	jsonStr := fmt.Sprintf(`{"%s":"bob@example.com","%s":"Bobby","%s":250.5,"%s":19.95}`,
		PersistentField[string](acct.Email).Name(),
		PersistentField[string](acct.Nickname).Name(),
		PersistentField[float64](ord.Amount).Name(),
		PersistentField[float64](oi.UnitPrice).Name(),
	)

	res := s.Validate(jsonStr)
	require.False(t, res.IsError(), "validation should succeed")
	vo := res.MustGet()

	var expected map[string]any
	require.NoError(t, json.Unmarshal([]byte(jsonStr), &expected))

	for _, f := range s.fields {
		exp, ok := expected[f.Name()]
		require.True(t, ok, "test JSON must contain key %s", f.Name())

		valOpt := vo.Get(f.UniqueName())
		require.True(t, valOpt.IsPresent(), "expected value present for %s (unique: %s)", f.Name(), f.UniqueName())
		got := valOpt.MustGet()
		require.Equal(t, exp, got)
	}
}

func TestValueObject_BackendQualifiedKeys_ViewOnlyFields(t *testing.T) {
	// Compose a schema mixing view-only fields (created via Field()) and persistent ones
	viewA := Field[string]("user_name")
	viewB := Field[int]("user_age")
	s := WithFields(viewA, viewB)

	jsonStr := `{"user_name":"Alice","user_age":30}`
	res := s.Validate(jsonStr)
	require.False(t, res.IsError())
	vo := res.MustGet()

	// For view-only fields QualifiedName() is equal to the stored qualifiedName and
	// Name() is the last segment. Ensure backend mapping by QualifiedName works.
	backend := make(map[string]any)
	for _, f := range s.fields {
		backend[f.QualifiedName()] = vo.Get(f.Name()).MustGet()
	}

	require.Equal(t, "Alice", backend[viewA.QualifiedName()])
	require.Equal(t, 30, backend[viewB.QualifiedName()])
}

func TestValidate_WithXQLFields_FromTestdata_ProducesQualifiedKeys(t *testing.T) {
	// Build schema using persistent fields
	s := WithXQLFields(acct.Email, acct.Nickname, ord.Amount, oi.UnitPrice)

	// Read testdata file
	jsonData, err := os.ReadFile("testdata/view_xql_valid.json")
	require.NoError(t, err)

	res := s.Validate(string(jsonData))
	require.False(t, res.IsError(), "expected validation to succeed")
	vo := res.MustGet()

	// The testdata file contains keys using the view names (last segment). Ensure
	// that values stored under the field's UniqueName() match the expected values
	// from the testdata.
	var m map[string]any
	require.NoError(t, json.Unmarshal(jsonData, &m))
	for _, f := range s.fields {
		viewName := f.Name()
		exp, ok := m[viewName]
		require.True(t, ok, "expected testdata to contain key %s", viewName)
		valOpt := vo.Get(f.UniqueName())
		require.True(t, valOpt.IsPresent(), "expected value for %s", viewName)
		require.Equal(t, exp, valOpt.MustGet())
	}
}
