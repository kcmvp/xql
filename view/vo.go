package view

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kcmvp/xql"
	"github.com/kcmvp/xql/internal"
	"github.com/kcmvp/xql/sqlx"
	"github.com/kcmvp/xql/validator"
	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/tidwall/gjson"
)

// timeLayouts defines the supported time formats for parsing time.Time fields.
var timeLayouts = []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"}

// validationError is a custom error type that holds a map of validation errors,
// ensuring that there is only one error per field.
type validationError struct {
	errors map[string]error
}

// Error implements the error interface, formatting all contained errors.
func (e *validationError) Error() string {
	if e == nil || len(e.errors) == 0 {
		return ""
	}
	// Sort keys for deterministic error messages, which is good for testing.
	keys := make([]string, 0, len(e.errors))
	for k := range e.errors {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("validation failed with the following errors:")
	for _, k := range keys {
		b.WriteString(fmt.Sprintf("- %s: %s", k, e.errors[k]))
	}
	return b.String()
}

// add adds a new error to the map.
func (e *validationError) add(fieldName string, err error) {
	if err != nil {
		if e.errors == nil {
			e.errors = make(map[string]error)
		}
		e.errors[fieldName] = err
	}
}

// err returns the validationError as a single error if it contains any errors.
func (e *validationError) err() error {
	if e == nil || len(e.errors) == 0 {
		return nil
	}
	return e
}

// ViewField is an internal, non-generic interface that allows Schema
// to hold a collection of fields with different underlying generic types.
//
// This is view-layer specific (validation/parsing behavior) and should not
// be confused with persistence metadata (for example types defined in `xql`).
type ViewField interface {
	// Scope Persistence-related accessors (delegated to backing xql.Field when present)
	Scope() string
	QualifiedName() string
	// Name View-specific accessors
	Name() string
	UniqueName() string
	IsArray() bool
	IsObject() bool
	Required() bool
	validate(node gjson.Result) mo.Result[any]
	validateRaw(v string) mo.Result[any]
	embeddedObject() mo.Option[*Schema]
}

type JSONField[T validator.FieldType] struct {
	qualifiedName string
	scope         string
	required      bool
	array         bool
	object        bool
	embedded      *Schema
	validators    []validator.Validator[T]
}

// JSONField implements ViewField and optionally wraps a persistent `xql.Field`.
// We copy minimal metadata (qualifiedName and scope) at WrapField time so the
// view layer is decoupled from persistent implementation details.

func (f *JSONField[T]) Required() bool {
	return f.required
}

func (f *JSONField[T]) IsArray() bool {
	return f.array
}

func (f *JSONField[T]) IsObject() bool {
	return f.object
}

func (f *JSONField[T]) embeddedObject() mo.Option[*Schema] {
	return lo.Ternary(f.embedded == nil, mo.None[*Schema](), mo.Some(f.embedded))
}

var _ ViewField = (*JSONField[string])(nil)

// Note: ViewField is sealed via unexported methods, so only types defined in
// this package implement it. Callers should pass ViewField values to
// `WithFields` when constructing a Schema.

func (f *JSONField[T]) Name() string {
	// Name returns the last segment of the qualifiedName split by '.'.
	q := f.qualifiedName
	if q == "" {
		return ""
	}
	if i := strings.LastIndex(q, "."); i != -1 {
		return q[i+1:]
	}
	return q
}

// Scope returns the stored scope for the view field (may be empty).
func (f *JSONField[T]) Scope() string {
	return f.scope
}

// QualifiedName returns the stored qualifiedName for the view field.
func (f *JSONField[T]) QualifiedName() string {
	return f.qualifiedName
}

// UniqueName returns the canonical storage key for this field. For persistent-backed
// fields this is the full qualified name (e.g. "table.column.view"). For view-only
// fields it falls back to the view key.
func (f *JSONField[T]) UniqueName() string {
	if f.qualifiedName != "" {
		return f.qualifiedName
	}
	return f.Name()
}

func (f *JSONField[T]) Optional() *JSONField[T] {
	f.required = false
	return f
}

// AsObject marks the JSONField as an embedded object and returns the field
// so callers can chain: PersistentField(...).AsObject()
func (f *JSONField[T]) AsObject() *JSONField[T] {
	f.object = true
	return f
}

// AsArray marks the JSONField as an array and returns the field so callers
// can chain: PersistentField(...).AsArray()
func (f *JSONField[T]) AsArray() *JSONField[T] {
	f.array = true
	return f
}

func (f *JSONField[T]) validateRaw(v string) mo.Result[any] {
	// typedString[T] returns mo.Result[T]
	// validateRaw needs to return mo.Result[any]
	typedValResult := typedString[T](v)
	if typedValResult.IsError() {
		// Wrap the error to provide more context about the field.
		err := fmt.Errorf("field '%s': %w", f.Name(), typedValResult.Error())
		return mo.Err[any](err)
	}

	val := typedValResult.MustGet()
	// Run validators on the successfully parsed value.
	for _, vfn := range f.validators {
		if err := vfn(val); err != nil {
			err = fmt.Errorf("field '%s': %w", f.Name(), err)
			return mo.Err[any](err)
		}
	}

	return mo.Ok[any](val)
}

// Validate checks the given raw string for the field. It returns a Result monad
// containing the typedJson value or an error
func (f *JSONField[T]) validate(node gjson.Result) mo.Result[any] {
	// Case: Nested Single Object
	if f.IsObject() && !f.IsArray() {
		// Recursively validate. The result will be a mo.Result[ValueObject].
		nestedResult := f.embeddedObject().MustGet().Validate(node.Raw)
		if nestedResult.IsError() {
			// Wrap the error to provide context.
			return mo.Err[any](fmt.Errorf("field '%s' validation failed, %w", f.Name(), nestedResult.Error()))
		}
		// Return the embedded ValueObject itself.
		return mo.Ok[any](nestedResult.MustGet())
	}

	// Case: Array
	if f.IsArray() {
		if !node.IsArray() {
			return mo.Err[any](fmt.Errorf("xql: field '%s' expected a JSON array but got Clause", f.Name()))
		}
		errs := &validationError{}
		// Subcase: Array of Objects
		if f.embeddedObject().IsPresent() {
			var values []ValueObject
			node.ForEach(func(index, element gjson.Result) bool {
				if !element.IsObject() {
					errs.add(fmt.Sprintf("%s[%d]", f.Name(), index.Int()), fmt.Errorf("expected a JSON object but got Clause"))
					return true // continue
				}
				result := f.embedded.Validate(element.Raw)
				if result.IsError() {
					// To avoid embedded error messages, if the embedded validation returns a
					// validationError with a single underlying error, we extract it.
					// This makes the final error message cleaner.
					errToAdd := result.Error()
					var nested *validationError
					if errors.As(errToAdd, &nested) && len(nested.errors) == 1 {
						for _, v := range nested.errors {
							errToAdd = v
						}
					}
					errs.add(fmt.Sprintf("%s[%d]", f.Name(), index.Int()), errToAdd)
				} else if errs.err() == nil {
					values = append(values, result.MustGet())
				}
				return true // continue
			})
			return lo.Ternary(errs.err() != nil, mo.Err[any](errs.err()), mo.Ok[any](values))
		}

		// Subcase: Array of Primitives
		var values []T
		node.ForEach(func(index, element gjson.Result) bool {
			// We need to validate each element of the array.
			typedVal := typedJson[T](element)
			if typedVal.IsError() {
				errs.add(fmt.Sprintf("%s[%d]", f.Name(), index.Int()), typedVal.Error())
				return true // continue to collect all errors
			}

			val := typedVal.MustGet()
			// Run validators on each element
			for _, v := range f.validators {
				if err := v(val); err != nil {
					errs.add(fmt.Sprintf("%s[%d]", f.Name(), index.Int()), err)
				}
			}

			// Only append if there were no errors for this specific element
			if errs.err() == nil {
				values = append(values, val)
			}
			return true
		})
		return lo.Ternary(errs.err() != nil, mo.Err[any](errs.err()), mo.Ok[any](values))
	}
	// --- Fallback for simple, non-array, non-object fields ---
	typedVal := typedJson[T](node)
	if typedVal.IsError() {
		err := fmt.Errorf("field '%s': %w", f.Name(), typedVal.Error())
		return mo.Err[any](err)
	}
	val := typedVal.MustGet()
	for _, v := range f.validators {
		if err := v(val); err != nil {
			err = fmt.Errorf("field '%s': %w", f.Name(), err)
			return mo.Err[any](err)
		}
	}
	return mo.Ok[any](val)
}

// typedJson attempts to convert a gjson.Result into the specified FieldType.
// It returns a mo.Result[T] which contains the typedJson value on success,
// or an error if the type conversion fails or the raw type does not match
// the expected Go type.
func typedJson[T validator.FieldType](res gjson.Result) mo.Result[T] {
	var zero T
	targetType := reflect.TypeOf(zero)

	switch targetType.Kind() {
	case reflect.String:
		if res.Type == gjson.String {
			return mo.Ok(any(res.String()).(T))
		}
	case reflect.Bool:
		if res.Type == gjson.True || res.Type == gjson.False {
			return mo.Ok(any(res.Bool()).(T))
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if res.Type != gjson.Number {
			break // Fall through to the default error at the end.
		}
		// To detect overflow and prevent floats, we get the int value, format it back
		// to a string, and compare it with the raw input. If they differ, it means
		// gjson saturated the value (overflow) or truncated a float.
		val := res.Int()
		if strconv.FormatInt(val, 10) != res.Raw {
			if strings.Contains(res.Raw, ".") {
				return mo.Err[T](fmt.Errorf("%w: cannot assign float value %s to integer type", validator.ErrTypeMismatch, res.Raw))
			}
			return mo.Err[T](overflowError(zero))
		}
		// Now check if the int64 value overflows the specific target type (e.g., int8, int16).
		if reflect.New(targetType).Elem().OverflowInt(val) {
			return mo.Err[T](overflowError(zero))
		}
		return mo.Ok(reflect.ValueOf(val).Convert(targetType).Interface().(T))

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if res.Type != gjson.Number {
			break
		}
		// Check for negative numbers, which is an overflow for unsigned types.
		if strings.Contains(res.Raw, "-") {
			return mo.Err[T](overflowError(zero))
		}
		// Similar to the signed int case, we compare string representations to
		// detect saturation on overflow or truncation of floats.
		val := res.Uint()
		if strconv.FormatUint(val, 10) != res.Raw {
			if strings.Contains(res.Raw, ".") {
				return mo.Err[T](fmt.Errorf("%w: cannot assign float value %s to unsigned integer type", validator.ErrTypeMismatch, res.Raw))
			}
			return mo.Err[T](overflowError(zero))
		}
		// Now check if the uint64 value overflows the specific target type (e.g., uint8, uint16).
		if reflect.New(targetType).Elem().OverflowUint(val) {
			return mo.Err[T](overflowError(zero))
		}
		return mo.Ok(reflect.ValueOf(val).Convert(targetType).Interface().(T))

	case reflect.Float32, reflect.Float64:
		var val float64
		var err error
		if res.Type == gjson.Number {
			val = res.Float()
		} else if res.Type == gjson.String {
			// Explicitly parse string to float, capturing any errors.
			val, err = strconv.ParseFloat(res.String(), 64)
			if err != nil {
				return mo.Err[T](fmt.Errorf("could not parse string '%s' as float: %w", res.String(), err))
			}
		} else {
			// For any other type, fall through to the default type mismatch error.
			break
		}
		if reflect.New(targetType).Elem().OverflowFloat(val) {
			return mo.Err[T](fmt.Errorf("value %f overflows type %T", val, zero))
		}
		return mo.Ok(reflect.ValueOf(val).Convert(targetType).Interface().(T))

	case reflect.Struct:
		if targetType == reflect.TypeOf(time.Time{}) {
			if res.Type == gjson.String {
				dateStr := res.String()
				for _, layout := range timeLayouts {
					if t, err := time.Parse(layout, dateStr); err == nil {
						return mo.Ok(any(t).(T))
					}
				}
				return mo.Err[T](fmt.Errorf("incorrect date format for string '%s'", res.String()))
			}
			break
		}
		fallthrough
	default:
		return mo.Err[T](fmt.Errorf("%w: unsupported type %T", validator.ErrTypeMismatch, zero))
	}

	// Default error for unhandled or mismatched types.
	return mo.Err[T](fmt.Errorf("%w: expected %T but got raw type %s", validator.ErrTypeMismatch, zero, res.Type))
}

// typedString attempts to convert a string into the specified FieldType.
// It returns a mo.Result[T] which contains the typed value on success,
// or an error if the type conversion fails or the string cannot be parsed
// into the expected Go type.
func typedString[T validator.FieldType](s string) mo.Result[T] {
	v, err := validator.ParseStringTo[T](s)
	if err != nil {
		return mo.Err[T](err)
	}
	return mo.Ok(v)
}

// overflowError creates a standard error for integer overflow.
func overflowError[T any](v T) error {
	return validator.OverflowError(v)
}

// ObjectField creates a slice of SchemaField for a embeddedObject object.
// It takes the name of the object field and a Schema representing its schema.
// Each field in the embeddedObject Schema will be prefixed with the object's name.
// The name of the object field should not contain '#' and `.`.
func ObjectField(name string, nested *Schema) *JSONField[string] {
	lo.Assertf(nested != nil, "Nested Schema is null for ObjectField %s", name)
	return trait[string](name, false, true, nested)
}

// ArrayOfObjectField creates a slice of SchemaField for an array of embeddedObject objects.
// It takes the name of the array field and a Schema representing the schema of its elements.
// The name of the array field should not contain '#' and `.`.
func ArrayOfObjectField(name string, nested *Schema) *JSONField[string] {
	lo.Assertf(nested != nil, "Nested Schema is null for ArrayOfObjectField %s", name)
	return trait[string](name, true, true, nested)
}

// ArrayField creates a FieldFunc for an array field.
// It is intended to be used for array fields that contain primitive types.
// The name of the array field should not contain '#' and `.`.
func ArrayField[T validator.FieldType](name string, vfs ...validator.ValidateFunc[T]) *JSONField[T] {
	return trait[T](name, true, false, nil, vfs...)
}

// Field creates a FieldFunc for a single field.
// It takes the name of the field and an optional list of validators.
// The returned FieldFunc can then be used to create a JSONField,
// allowing for additional validators to be chained.
// The name of the field should not contain '#' and `.`.
func Field[T validator.FieldType](name string, vfs ...validator.ValidateFunc[T]) *JSONField[T] {
	return trait[T](name, false, false, nil, vfs...)
}

func trait[T validator.FieldType](name string, isArray, isObject bool, nested *Schema, vfs ...validator.ValidateFunc[T]) *JSONField[T] {
	if strings.ContainsAny(name, ".#") {
		panic(fmt.Sprintf("xql: field name '%s' cannot contain '.' or '#'", name))
	}
	names := make(map[string]struct{})
	var nf []validator.Validator[T]
	for _, v := range vfs {
		n, f := v()
		if _, exists := names[n]; exists {
			panic(fmt.Sprintf("xql: duplicate validator '%s' for field '%s'", n, name))
		}
		names[n] = struct{}{}
		nf = append(nf, f)
	}
	return &JSONField[T]{
		qualifiedName: name, // view-only fields: qualifiedName is the view key
		scope:         "",
		array:         isArray,
		object:        isObject,
		embedded:      nested,
		validators:    nf,
		required:      true,
	}
}

// PersistentField builds a view `JSONField[T]` from a persistent `xql.PersistentField[T]`.
//
// It copies persistence metadata (qualified name and scope) and merges validators from the
// persistent definition with any view-layer validators provided. If a validator with the
// same name is supplied by both the persistent field and the view layer, this function
// will panic to prevent ambiguous validation behavior.
//
// The returned `*JSONField[T]` is a view-layer wrapper that performs parsing and
// validation appropriate for HTTP/JSON inputs while carrying the persistence QualifiedName
// and Scope so the validated `ValueObject` can be mapped back to persistence layer keys.
func PersistentField[T validator.FieldType](f *xql.PersistentField[T], vfs ...validator.ValidateFunc[T]) *JSONField[T] {
	if f == nil {
		panic("view: PersistentField requires a non-nil *xql.PersistentField[T]")
	}

	var validators []validator.Validator[T]
	// name set used to detect duplicate validator names across persistent and view validators
	names := make(map[string]struct{})

	// Include validators from the persistent field first
	for _, vf := range f.Constraints() {
		name, fn := vf()
		if _, exists := names[name]; exists {
			panic(fmt.Sprintf("xql: duplicate validator '%s' from persistent field in PersistentField", name))
		}
		names[name] = struct{}{}
		fnLocal := fn
		validators = append(validators, func(v T) error { return fnLocal(v) })
	}

	// Convert view-provided validator factory functions into concrete validators.
	for _, vf := range vfs {
		name, fn := vf()
		if _, ok := names[name]; ok {
			panic(fmt.Sprintf("xql: duplicate validator '%s' in PersistentField", name))
		}
		names[name] = struct{}{}
		fnLocal := fn
		validators = append(validators, func(v T) error { return fnLocal(v) })
	}

	return &JSONField[T]{
		qualifiedName: f.QualifiedName(),
		scope:         f.Scope(),
		required:      true,
		array:         false,
		object:        false,
		embedded:      nil,
		validators:    validators,
	}
}

// Schema is a blueprint for validating a raw object.
type Schema struct {
	fields             []ViewField
	allowUnknownFields bool
}

// WithFields constructs a Schema from the provided ViewField values.
// It validates that field names are unique and returns a Schema ready for
// validation operations.
func WithFields(fields ...ViewField) *Schema {
	// Defensive duplicate name check similar to previous behavior.
	names := make(map[string]struct{})
	for _, f := range fields {
		if _, exists := names[f.Name()]; exists {
			panic(fmt.Sprintf("xql: duplicate field name '%s' in Schema definition", f.Name()))
		}
		names[f.Name()] = struct{}{}
	}
	// New: ensure QualifiedName uniqueness for fields that provide one.
	qnames := make(map[string]struct{})
	for _, f := range fields {
		qn := f.QualifiedName()
		if qn == "" {
			continue
		}
		if _, exists := qnames[qn]; exists {
			panic(fmt.Sprintf("xql: duplicate qualified name '%s' in Schema definition", qn))
		}
		qnames[qn] = struct{}{}
	}
	return &Schema{fields: fields, allowUnknownFields: false}
}

// AllowUnknownFields is a fluent helper to enable acceptance of unknown JSON/url
// parameters during validation. It sets the flag on the Schema and returns the
// same Schema pointer for chaining.
func (s *Schema) AllowUnknownFields() *Schema {
	if s == nil {
		return s
	}
	s.allowUnknownFields = true
	return s
}

func (s *Schema) Extend(another *Schema) *Schema {
	// 1. Create a new field slice with enough capacity.
	newFields := make([]ViewField, 0, len(s.fields)+len(another.fields))

	// 2. Copy fields from both Schemas.
	newFields = append(newFields, s.fields...)
	newFields = append(newFields, another.fields...)

	// 3. Perform strict duplicate checking.
	names := make(map[string]struct{})
	for _, f := range newFields {
		if _, exists := names[f.Name()]; exists {
			panic(fmt.Sprintf("xql: duplicate field name '%s' found during Extend", f.Name()))
		}
		names[f.Name()] = struct{}{}
	}

	// 4. Return a new Schema with the combined fields.
	// If either of the original objects allowed unknown fields, the new one should too.
	return &Schema{
		fields:             newFields,
		allowUnknownFields: s.allowUnknownFields || another.allowUnknownFields,
	}
}

// ValueObject is a sealed interface for a type-safe map holding validated Schema.
// The seal method prevents implementations outside this package.
//
// All getter methods (String, Int, Get, etc.) support dot notation for hierarchical
// access to embedded objects and arrays.
//
// For example, given a ValueObject `vo` representing the JSON:
//
//	email := vo.MstString("user.email") // "test@example.com"
//
// itemID := vo.MstInt("items.0.id")   // 101
//
// If a path is invalid (e.g., key not found), the `Option`
// based getters (like `Clause`) will return `mo.None`, while the `Mst` prefixed
// getters (like `MstString`) will panic.
type ValueObject interface {
	internal.ValueObject
	// StringArray returns an Option containing a slice of strings for the given name.
	// It panics if the field exists but is not a []string.
	StringArray(name string) mo.Option[[]string]
	// MstStringArray returns a slice of strings for the given name.
	// It panics if the key is not found or the value is not a []string.
	MstStringArray(name string) []string
	// IntArray returns an Option containing a slice of ints for the given name.
	// It panics if the field exists but is not a []int.
	IntArray(name string) mo.Option[[]int]
	// MstIntArray returns a slice of ints for the given name.
	// It panics if the key is not found or the value is not a []int.
	MstIntArray(name string) []int
	// Int64Array returns an Option containing a slice of int64s for the given name.
	// It panics if the field exists but is not a []int64.
	Int64Array(name string) mo.Option[[]int64]
	// MstInt64Array returns a slice of int64s for the given name.
	// It panics if the key is not found or the value is not a []int64.
	MstInt64Array(name string) []int64
	// Float64Array returns an Option containing a slice of float64s for the given name.
	// It panics if the field exists but is not a []float64.
	Float64Array(name string) mo.Option[[]float64]
	// MstFloat64Array returns a slice of float64s for the given name.
	// It panics if the key is not found or the value is not a []float64.
	MstFloat64Array(name string) []float64
	// BoolArray returns an Option containing a slice of bools for the given name.
	// It panics if the field exists but is not a []bool.
	BoolArray(name string) mo.Option[[]bool]
	// MstBoolArray returns a slice of bools for the given name.
	// It panics if the key is not found or the value is not a []bool.
	MstBoolArray(name string) []bool
	// FlatMap converts the ValueObject into a flattened map keyed by dotted
	// qualified names (e.g. "table.column.view" or "table.column").
	FlatMap() sqlx.FlatMap
	seal()
}

// valueObject is the private, concrete implementation of the ValueObject interface.
// It is defined as a plain map so tests can use map literals and indexing directly.
// We forward method calls to internal.Data converters when necessary.
type valueObject struct {
	internal.Data
}

var _ ValueObject = (*valueObject)(nil)

// MarshalJSON ensures the valueObject is serialized as the underlying map
// (i.e. the embedded Data) instead of as a struct with a "Data" field.
func (vo valueObject) MarshalJSON() ([]byte, error) {
	return json.Marshal(vo.Data)
}

func (vo valueObject) seal() {}

// FlatMap converts the valueObject into a flattened map[string]any. It iterates over
// the structure recursively and produces dotted keys for nested fields.
func (vo valueObject) FlatMap() sqlx.FlatMap {
	out := make(sqlx.FlatMap)
	// recursive walker
	var walk func(prefix string, v any)
	walk = func(prefix string, v any) {
		switch val := v.(type) {
		case internal.Data:
			for k, vv := range val {
				nk := k
				if prefix != "" {
					nk = prefix + "." + k
				}
				walk(nk, vv)
			}
		case valueObject:
			// expose underlying Data for nested valueObject
			for _, fk := range val.Fields() {
				if opt := val.Get(fk); opt.IsPresent() {
					v := opt.MustGet()
					nk := fk
					if prefix != "" {
						nk = prefix + "." + fk
					}
					walk(nk, v)
				}
			}
		case map[string]any:
			for k, vv := range val {
				nk := k
				if prefix != "" {
					nk = prefix + "." + k
				}
				walk(nk, vv)
			}
		default:
			// arrays and primitives are stored as-is under the accumulated prefix
			if prefix != "" {
				out[prefix] = val
			}
		}
	}

	for _, k := range vo.Fields() {
		if opt := vo.Get(k); opt.IsPresent() {
			walk(k, opt.MustGet())
		}
	}
	return out
}

// setObjectField stores a validated value into the provided internal.Data map
// under the given key. It normalizes embedded object values so plain maps
// become the concrete view.valueObject type and preserves existing
// ValueObject implementations.
func setObjectField(object internal.Data, key string, val any) {
	switch v := val.(type) {
	case internal.Data:
		object[key] = valueObject{Data: v}
	case valueObject, *valueObject:
		object[key] = v
	case ValueObject:
		object[key] = v
	default:
		object[key] = val
	}
}

// setNestedField stores val into object under a dotted path key like "a.b.c".
// It will create nested internal.Data maps as needed. For the final value it
// uses setObjectField to normalize ValueObject/map types.
func setNestedField(object internal.Data, key string, val any) {
	parts := strings.Split(key, ".")
	if len(parts) == 0 {
		return
	}
	cur := object
	for i := 0; i < len(parts)-1; i++ {
		p := parts[i]
		// if existing value is a Data, descend
		if next, ok := cur[p]; ok {
			if m, ok := next.(internal.Data); ok {
				cur = m
				continue
			}
			// If existing value is a ValueObject, attempt to extract its Data
			if vo, ok := next.(ValueObject); ok {
				// Convert ValueObject to internal.Data by marshaling/getting Fields
				inner := internal.Data{}
				for _, k := range vo.Fields() {
					if opt := vo.Get(k); opt.IsPresent() {
						inner[k] = opt.MustGet()
					}
				}
				cur[p] = inner
				cur = inner
				continue
			}
			// If it's neither a map nor a ValueObject, overwrite with a new map
			m := internal.Data{}
			cur[p] = m
			cur = m
		} else {
			m := internal.Data{}
			cur[p] = m
			cur = m
		}
	}
	// final part
	final := parts[len(parts)-1]
	setObjectField(cur, final, val)
}

func (s *Schema) Validate(json string, urlParams ...map[string]string) mo.Result[ValueObject] {
	if len(json) > 0 && !gjson.Valid(json) {
		return mo.Err[ValueObject](fmt.Errorf("invalid json %s", json))
	}
	object := internal.Data{}
	errs := &validationError{}
	// Check for unknown fields first if not allowed.
	voFields := lo.SliceToMap(s.fields, func(field ViewField) (string, bool) {
		return field.Name(), field.IsArray() || field.IsObject()
	})
	urlPair := map[string]string{}
	for _, pair := range urlParams {
		for k, v := range pair {
			// self conflict check
			if _, ok := urlPair[k]; ok {
				errs.add(k, fmt.Errorf("duplicated url parameter '%s'", k))
			}
			if !s.allowUnknownFields {
				if nested, ok := voFields[k]; !ok {
					errs.add(k, fmt.Errorf("unknown url parameter '%s'", k))
				} else if nested {
					errs.add(k, fmt.Errorf("url parameter '%s' is mapped to a embedded object", k))
				}
			}
			urlPair[k] = v
		}
	}

	lo.ForEach(gjson.Get(json, "@keys").Array(), func(field gjson.Result, index int) {
		jsonKey := field.String()
		if _, ok := urlPair[jsonKey]; ok {
			errs.add(jsonKey, fmt.Errorf("duplicate parameter in url and json '%s'", jsonKey))
		}
		if !s.allowUnknownFields {
			if _, ok := voFields[jsonKey]; !ok {
				errs.add(jsonKey, fmt.Errorf("unknown json field '%s'", jsonKey))
			}
		}
	})

	// fail first for conflict
	if errs.err() != nil {
		return mo.Err[ValueObject](errs.err())
	}

	for _, field := range s.fields {
		var rs mo.Result[any]
		node := gjson.Get(json, field.Name())
		if !node.Exists() {
			// need to check in urlPair
			urlValue, ok := urlPair[field.Name()]
			if !ok {
				if field.Required() {
					errs.add(field.Name(), fmt.Errorf("%s %w", field.Name(), validator.ErrRequired))
				}
				continue
			}
			rs = field.validateRaw(urlValue)
		} else {
			rs = field.validate(node)
		}
		if rs.IsError() {
			// If the returned error is a validationError, it likely came from a
			// embedded validation (like an array). We should merge its errors
			// instead of nesting the error object, which would create ugly, duplicated messages.
			var nestedErr *validationError
			if errors.As(rs.Error(), &nestedErr) {
				for key, err := range nestedErr.errors {
					errs.add(key, err)
				}
			} else {
				errs.add(field.Name(), rs.Error())
			}
			continue
		}
		// Store the validated value as-is. For embedded objects the validate()
		// returns a ValueObject, so we keep it to preserve hierarchical access.
		val := rs.MustGet()
		// Use UniqueName() as the storage key so view validation maps back to
		// persistent field identifiers when available.
		key := field.UniqueName()
		// Store into nested map structure to support dot-path lookups via internal.Get
		setNestedField(object, key, val)
	}

	// Add unknown URL parameters to the final object if allowed.
	if s.allowUnknownFields {
		for k, v := range urlPair {
			if _, exists := object[k]; !exists {
				object[k] = v
			}
		}
	}
	return lo.Ternary(errs.err() != nil, mo.Err[ValueObject](errs.err()), mo.Ok[ValueObject](valueObject{
		Data: object,
	}))
}

// WithXQLFields builds a view `Schema` from one or more persistent `xql.Field` values.
//
// Behavior and contract:
//   - Each provided `xql.Field` must be a concrete `*xql.PersistentField[T]` produced
//     by generator code (common generated fields live under `sample/gen/field`).
//   - The function converts each persistent field into a view-layer `ViewField` by
//     creating a `PersistentField[T]` wrapper. Any validator factories attached to the
//     persistent field are carried into the resulting view field so view-layer
//     validation honors constraints declared at generation time.
//   - The resulting `Schema` validates JSON using the view/key (the persistent field's
//     view name, i.e. the last segment). After successful validation each value is
//     stored under the field's `UniqueName()` (which for persistent fields is the
//     qualified persistence identifier, e.g. "table.column.view"). This preserves the
//     mapping between view input and backend identifiers required by SQL helpers.
//   - If a provided field's concrete generic type is not supported by this conversion
//     (e.g. an unexpected underlying type), the call will panic with a descriptive
//     message.
//
// Example:
//
//	s := WithXQLFields(account.Email, account.Nickname)
//	res := s.Validate(`{"Email":"a@x.com","Nickname":"Joe"}`)
//
// Notes:
//   - This helper is intended for use with generator-produced `xql.Field` values.
func WithXQLFields(fields ...xql.Field) *Schema {
	vf := make([]ViewField, 0, len(fields))
	for _, f := range fields {
		switch concrete := f.(type) {
		case *xql.PersistentField[int]:
			vf = append(vf, PersistentField[int](concrete))
		case *xql.PersistentField[int8]:
			vf = append(vf, PersistentField[int8](concrete))
		case *xql.PersistentField[int16]:
			vf = append(vf, PersistentField[int16](concrete))
		case *xql.PersistentField[int32]:
			vf = append(vf, PersistentField[int32](concrete))
		case *xql.PersistentField[int64]:
			vf = append(vf, PersistentField[int64](concrete))
		case *xql.PersistentField[uint]:
			vf = append(vf, PersistentField[uint](concrete))
		case *xql.PersistentField[uint8]:
			vf = append(vf, PersistentField[uint8](concrete))
		case *xql.PersistentField[uint16]:
			vf = append(vf, PersistentField[uint16](concrete))
		case *xql.PersistentField[uint32]:
			vf = append(vf, PersistentField[uint32](concrete))
		case *xql.PersistentField[uint64]:
			vf = append(vf, PersistentField[uint64](concrete))
		case *xql.PersistentField[float32]:
			vf = append(vf, PersistentField[float32](concrete))
		case *xql.PersistentField[float64]:
			vf = append(vf, PersistentField[float64](concrete))
		case *xql.PersistentField[string]:
			vf = append(vf, PersistentField[string](concrete))
		case *xql.PersistentField[bool]:
			vf = append(vf, PersistentField[bool](concrete))
		case *xql.PersistentField[time.Time]:
			vf = append(vf, PersistentField[time.Time](concrete))
		default:
			panic(fmt.Sprintf("view: WithXQLFields: unsupported xql.Field concrete type %T", f))
		}
	}
	return WithFields(vf...)
}
