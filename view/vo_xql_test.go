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
	// Choose a few generated persistent fields and verify WrapField behavior
	fEmail := acct.Email
	vfEmail := WrapField[string](fEmail)
	require.Equal(t, fEmail.QualifiedName(), vfEmail.QualifiedName())
	require.Equal(t, fEmail.Scope(), vfEmail.Scope())
	require.Equal(t, "Email", vfEmail.Name())

	// Nickname has a max length validator in the generated field.
	nick := acct.Nickname
	vNick := WrapField[string](nick)
	long := strings.Repeat("a", 101)
	r := vNick.validateRaw(long)
	require.True(t, r.IsError(), "expected Nickname validator to reject long string")

	// Test WrapFieldAsArray / WrapFieldAsObject flags
	vfArray := WrapFieldAsArray[string](fEmail)
	require.True(t, vfArray.IsArray())
	vfObj := WrapFieldAsObject[string](prof.Bio)
	require.True(t, vfObj.IsObject())

	// Decimal constraint exists on orderitem.UnitPrice; ensure QualifiedName copies
	up := oi.UnitPrice
	vup := WrapField[float64](up)
	require.Equal(t, up.QualifiedName(), vup.QualifiedName())

	// WithXQLFields should produce a Schema equivalent to WithFields(WrapField(...))
	name := WrapField[string](acct.Email).Name()
	s1 := WithXQLFields(acct.Email)
	json := fmt.Sprintf(`{"%s":"john@example.com"}`, name)
	res := s1.Validate(json)
	require.False(t, res.IsError())

	s2 := WithFields(WrapField[string](acct.Email))
	res2 := s2.Validate(json)
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
	json := fmt.Sprintf(`{"Email":"alice@example.com","Nickname":"Al","Amount":123.45,"UnitPrice":9.99}`)
	res := s.Validate(json)
	require.False(t, res.IsError(), "expected combined schema to validate")

	// Now make a Nickname too long which should fail
	badJSON := fmt.Sprintf(`{"Email":"alice@example.com","Nickname":"%s","Amount":123.45,"UnitPrice":9.99}`, strings.Repeat("x", 201))
	res2 := s.Validate(badJSON)
	require.True(t, res2.IsError(), "expected validation to fail on long nickname")
}

func TestWrapField_NameExtraction(t *testing.T) {
	// Ensure Name() returns the last segment when QualifiedName contains schema.table
	// Simulate by using a generated field which has table possibly set with schema.
	// We can't change generated table, but ensure Name extracts last segment.
	f := ord.CreatedAt
	vf := WrapField[time.Time](f)
	require.Equal(t, "CreatedAt", vf.Name())
}

func TestWrapField_DuplicateValidatorPanics(t *testing.T) {
	// acct.Nickname has a persistent validator 'max_length' (100). Adding another
	// max_length via a view validator should cause WrapField to panic due to duplicate name.
	n := acct.Nickname
	require.Panics(t, func() {
		_ = WrapField[string](n, validator.MaxLength(50))
	})
}

func TestWrapField_NilPanics(t *testing.T) {
	// Passing a nil persistent field should panic
	require.Panics(t, func() {
		_ = WrapField[string](nil)
	})
}

func TestWrapField_MergesValidators(t *testing.T) {
	// Wrap persistent Nickname and add an additional min-length validator via view
	n := acct.Nickname
	vf := WrapField[string](n, validator.MinLength(2))
	// Too short should fail (min length 2)
	r := vf.validateRaw("A")
	require.True(t, r.IsError())
	// Valid length and under persistent max should pass
	r2 := vf.validateRaw("Abc")
	require.False(t, r2.IsError())
}

func TestWrapField_ArrayValidation(t *testing.T) {
	// Wrap Quantity as array and validate using WithFields
	q := WrapFieldAsArray[int64](oi.Quantity)
	s := WithFields(q)
	json := fmt.Sprintf(`{"%s":[1,2,3]}`, q.Name())
	res := s.Validate(json)
	require.False(t, res.IsError())
	// Non-array should fail
	bad := fmt.Sprintf(`{"%s": 123}`, q.Name())
	res2 := s.Validate(bad)
	require.True(t, res2.IsError())
}

func TestValueObject_BackendQualifiedKeys_HappyPath(t *testing.T) {
	// Build schema from generated persistent fields
	s := WithXQLFields(acct.Email, acct.Nickname, ord.Amount, oi.UnitPrice)

	// Use JSON with view names (Name()) as keys
	json := fmt.Sprintf(`{"%s":"bob@example.com","%s":"Bobby","%s":250.5,"%s":19.95}`,
		WrapField[string](acct.Email).Name(),
		WrapField[string](acct.Nickname).Name(),
		WrapField[float64](ord.Amount).Name(),
		WrapField[float64](oi.UnitPrice).Name(),
	)

	res := s.Validate(json)
	require.False(t, res.IsError(), "validation should succeed")
	vo := res.MustGet()

	// Convert to backend map keyed by QualifiedName()
	backend := make(map[string]any)
	for _, f := range s.fields {
		// use the view name to retrieve value from vo
		name := f.Name()
		valOpt := vo.Get(name)
		if !valOpt.IsPresent() {
			// missing optional fields are OK
			continue
		}
		v := valOpt.MustGet()
		backend[f.QualifiedName()] = v
	}

	// Check backend map has entries keyed by QualifiedName
	for _, f := range s.fields {
		val, ok := backend[f.QualifiedName()]
		require.True(t, ok, "backend key %s must exist", f.QualifiedName())
		// Also assert the stored value equals the one in the ValueObject via Name()
		name := f.Name()
		voVal := vo.Get(name).MustGet()
		require.Equal(t, voVal, val)
	}
}

func TestValueObject_BackendQualifiedKeys_ViewOnlyFields(t *testing.T) {
	// Compose a schema mixing view-only fields (created via Field()) and persistent ones
	viewA := Field[string]("user_name")
	viewB := Field[int]("user_age")
	s := WithFields(viewA, viewB)

	json := `{"user_name":"Alice","user_age":30}`
	res := s.Validate(json)
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

	// Also assert directly that the Email value can be retrieved via the view name
	//emailName := WrapField[string](acct.Email).Name()
	emailName := acct.Email.View()
	emailOpt := vo.Get(emailName)
	require.True(t, emailOpt.IsPresent(), "expected Email to be present in ValueObject via Name()")
	require.Equal(t, "bob@example.com", emailOpt.MustGet())

	// The current implementation stores values keyed by the view Name() (the
	// last segment of QualifiedName) which may differ from the original field
	// QualifiedName if a schema is involved. This test ensures the stored value
	// can be retrieved using the view name and matches the testdata.
	var m map[string]any
	require.NoError(t, json.Unmarshal(jsonData, &m))
	for _, f := range s.fields {
		viewName := f.Name()
		exp, ok := m[viewName]
		require.True(t, ok, "expected testdata to contain key %s", viewName)
		valOpt := vo.Get(viewName)
		require.True(t, valOpt.IsPresent(), "expected value for %s", viewName)
		require.Equal(t, exp, valOpt.MustGet())
	}
}
