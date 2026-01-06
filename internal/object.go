package internal

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/samber/mo"
)

type ValueObject interface {
	String(name string) mo.Option[string]
	MstString(name string) string
	Int(name string) mo.Option[int]
	MstInt(name string) int
	Int8(name string) mo.Option[int8]
	MstInt8(name string) int8
	Int16(name string) mo.Option[int16]
	MstInt16(name string) int16
	Int32(name string) mo.Option[int32]
	MstInt32(name string) int32
	Int64(name string) mo.Option[int64]
	MstInt64(name string) int64
	Uint(name string) mo.Option[uint]
	MstUint(name string) uint
	Uint8(name string) mo.Option[uint8]
	MstUint8(name string) uint8
	Uint16(name string) mo.Option[uint16]
	MstUint16(name string) uint16
	Uint32(name string) mo.Option[uint32]
	MstUint32(name string) uint32
	Uint64(name string) mo.Option[uint64]
	MstUint64(name string) uint64
	Float64(name string) mo.Option[float64]
	MstFloat64(name string) float64
	Float32(name string) mo.Option[float32]
	MstFloat32(name string) float32
	Bool(name string) mo.Option[bool]
	MstBool(name string) bool
	Time(name string) mo.Option[time.Time]
	MstTime(name string) time.Time
	Get(string) mo.Option[any]
	Add(name string, value any)
	Update(name string, value any)
	// Fields returns the list of field names of the value object.
	Fields() []string
}

type Data map[string]any

var _ ValueObject = (*Data)(nil)

func (vo Data) Get(s string) mo.Option[any] {
	return Get[any](vo, s)
}

// Keys returns all top-level keys present in the value object. The list is
// sorted to ensure a deterministic order for callers and tests.
func (vo Data) Fields() []string {
	ks := make([]string, 0, len(vo))
	for k := range vo {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// Add adds a new property to the value object.
// It panics if the property already exists or if the name contains '.'.
func (vo Data) Add(name string, value any) {
	_, ok := vo[name]
	lo.Assertf(!ok, "xql: property '%s' already exists", name)
	lo.Assertf(!strings.Contains(name, "."), "xql: property '%s' contains '.'", name)
	vo[name] = value
}

// Update modifies an existing property in the value object.
// It panics if the property does not exist.
func (vo Data) Update(name string, value any) {
	if _, ok := vo[name]; !ok {
		panic(fmt.Sprintf("xql: property '%s' does not exist", name))
	}
	vo[name] = value
}

var _ ValueObject = (*Data)(nil)

// seal is an empty method to satisfy the sealed ValueObject interface.
func (vo Data) seal() {}

// Get is a generic helper to retrieve a value and assert its type.
// It returns an Option, which will be empty if the key was not present.
// It panics if the key exists but the type is incorrect. This function
// supports dot notation for embedded objects and array indexing (e.g., "field.0.nestedField").
func Get[T any](data Data, name string) mo.Option[T] {
	if val, ok := data[name]; ok {
		typedValue, ok := val.(T)
		lo.Assertf(ok, "xql: field '%s' has wrong type: expected %T, got %T", name, *new(T), val)
		return mo.Some(typedValue)
	}
	parts := strings.Split(name, ".")
	var currentValue any = data
	for _, part := range parts {
		if currentValue == nil {
			return mo.None[T]()
		}
		// If it's a map (plain Data), look up the key first to avoid delegating
		// to ValueObject which may call back into this Get function and cause recursion.
		if voMap, ok := currentValue.(Data); ok {
			nextValue, exists := voMap[part]
			if !exists {
				return mo.None[T]()
			}
			currentValue = nextValue
			continue
		}
		// If the current value implements ValueObject (but is not plain Data),
		// delegate the lookup to its Get method for proper hierarchical traversal.
		if voIface, ok := currentValue.(ValueObject); ok {
			opt := voIface.Get(part)
			if !opt.IsPresent() {
				return mo.None[T]()
			}
			currentValue = opt.MustGet()
			continue
		}
		// If it's a slice, look up the index.
		val := reflect.ValueOf(currentValue)
		if val.Kind() == reflect.Slice {
			index, err := strconv.Atoi(part)
			lo.Assertf(err == nil, "xql: path part '%s' in '%s' is not a valid integer index for a slice", part, name)
			lo.Assertf(index >= 0 && index < val.Len(), "xql: array bound exceed: %v", val)
			currentValue = val.Index(index).Interface()
			continue
		}
		// If we are here, we are trying to traverse into a primitive from a non-final path segment.
		return mo.None[T]()
	}

	typedValue, ok := currentValue.(T)
	lo.Assertf(ok, "xql: field '%s' has wrong type: expected %T, got %T", name, *new(T), currentValue)
	return mo.Some(typedValue)
}

// String returns an Option containing the string value for the given name.
// It panics if the field exists but is not a string.
func (vo Data) String(name string) mo.Option[string] {
	return Get[string](vo, name)
}

// MstString returns the string value for the given name.
// It panics if the key is not found or the value is not a string.
func (vo Data) MstString(name string) string {
	return vo.String(name).MustGet()
}

// Int returns an Option containing the int value for the given name.
// It panics if the field exists but is not an int.
func (vo Data) Int(name string) mo.Option[int] {
	return Get[int](vo, name)
}

// MstInt returns the int value for the given name.
// It panics if the key is not found or the value is not an int.
func (vo Data) MstInt(name string) int {
	return vo.Int(name).MustGet()
}

// Int8 returns an Option containing the int8 value for the given name.
// It panics if the field exists but is not an int8.
func (vo Data) Int8(name string) mo.Option[int8] {
	return Get[int8](vo, name)
}

// MstInt8 returns the int8 value for the given name.
// It panics if the key is not found or the value is not an int8.
func (vo Data) MstInt8(name string) int8 {
	return vo.Int8(name).MustGet()
}

// Int16 returns an Option containing the int16 value for the given name.
// It panics if the field exists but is not an int16.
func (vo Data) Int16(name string) mo.Option[int16] {
	return Get[int16](vo, name)
}

// MstInt16 returns the int16 value for the given name.
// It panics if the key is not found or the value is not an int16.
func (vo Data) MstInt16(name string) int16 {
	return vo.Int16(name).MustGet()
}

// Int32 returns an Option containing the int32 value for the given name.
// It panics if the field exists but is not an int32.
func (vo Data) Int32(name string) mo.Option[int32] {
	return Get[int32](vo, name)
}

// MstInt32 returns the int32 value for the given name.
// It panics if the key is not found or the value is not an int32.
func (vo Data) MstInt32(name string) int32 {
	return vo.Int32(name).MustGet()
}

// Int64 returns an Option containing the int64 value for the given name.
// It panics if the field exists but is not an int64.
func (vo Data) Int64(name string) mo.Option[int64] {
	return Get[int64](vo, name)
}

// MstInt64 returns the int64 value for the given name.
// It panics if the key is not found or the value is not an int64.
func (vo Data) MstInt64(name string) int64 {
	return vo.Int64(name).MustGet()
}

// Uint returns an Option containing the uint value for the given name.
// It panics if the field exists but is not a uint.
func (vo Data) Uint(name string) mo.Option[uint] {
	return Get[uint](vo, name)
}

// MstUint returns the uint value for the given name.
// It panics if the key is not found or the value is not a uint.
func (vo Data) MstUint(name string) uint {
	return vo.Uint(name).MustGet()
}

// Uint8 returns an Option containing the uint8 value for the given name.
// It panics if the field exists but is not an unit8.
func (vo Data) Uint8(name string) mo.Option[uint8] {
	return Get[uint8](vo, name)
}

// MstUint8 returns the uint8 value for the given name.
// It panics if the key is not found or the value is not an unit8.
func (vo Data) MstUint8(name string) uint8 {
	return vo.Uint8(name).MustGet()
}

// Uint16 returns an Option containing the uint16 value for the given name.
// It panics if the field exists but is not an unit16.
func (vo Data) Uint16(name string) mo.Option[uint16] {
	return Get[uint16](vo, name)
}

// MstUint16 returns the uint16 value for the given name.
// It panics if the key is not found or the value is not an unit16.
func (vo Data) MstUint16(name string) uint16 {
	return vo.Uint16(name).MustGet()
}

// Uint32 returns an Option containing the uint32 value for the given name.
// It panics if the field exists but is not an unit32.
func (vo Data) Uint32(name string) mo.Option[uint32] {
	return Get[uint32](vo, name)
}

// MstUint32 returns the uint32 value for the given name.
// It panics if the key is not found or the value is not an unit32.
func (vo Data) MstUint32(name string) uint32 {
	return vo.Uint32(name).MustGet()
}

// Uint64 returns an Option containing the uint64 value for the given name.
// It panics if the field exists but is not an unit64.
func (vo Data) Uint64(name string) mo.Option[uint64] {
	return Get[uint64](vo, name)
}

// MstUint64 returns the uint64 value for the given name.
// It panics if the key is not found or the value is not an unit64.
func (vo Data) MstUint64(name string) uint64 {
	return vo.Uint64(name).MustGet()
}

// Float64 Float returns an Option containing the float64 value for the given name.
// It panics if the field exists but is not a float64.
func (vo Data) Float64(name string) mo.Option[float64] {
	return Get[float64](vo, name)
}

// MstFloat64 returns the float64 value for the given name.
// It panics if the key is not found or the value is not a float64.
func (vo Data) MstFloat64(name string) float64 {
	return vo.Float64(name).MustGet()
}

// Float32 returns an Option containing the float32 value for the given name.
// It panics if the field exists but is not a float32.
func (vo Data) Float32(name string) mo.Option[float32] {
	return Get[float32](vo, name)
}

// MstFloat32 returns the float32 value for the given name.
// It panics if the key is not found or the value is not a float32.
func (vo Data) MstFloat32(name string) float32 {
	return vo.Float32(name).MustGet()
}

// Bool returns an Option containing the bool value for the given name.
// It panics if the field exists but is not a bool.
func (vo Data) Bool(name string) mo.Option[bool] {
	return Get[bool](vo, name)
}

// MstBool returns the bool value for the given name.
// It panics if the key is not found or the value is not a bool.
func (vo Data) MstBool(name string) bool {
	return vo.Bool(name).MustGet()
}

// Time returns an Option containing the time.Time value for the given name.
// It panics if the field exists but is not a time.Time.
func (vo Data) Time(name string) mo.Option[time.Time] {
	return Get[time.Time](vo, name)
}

// MstTime returns the time.Time value for the given name.
// It panics if the key is not found or the value is not a time.Time.
func (vo Data) MstTime(name string) time.Time {
	return vo.Time(name).MustGet()
}

// StringArray returns an Option containing the []string value for the given name.
// It panics if the field exists but is not a []string.
func (vo Data) StringArray(name string) mo.Option[[]string] {
	return Get[[]string](vo, name)
}

// MstStringArray returns the []string value for the given name.
// It panics if the key is not found or the value is not a []string.
func (vo Data) MstStringArray(name string) []string {
	return vo.StringArray(name).MustGet()
}

// IntArray returns an Option containing the []int value for the given name.
// It panics if the field exists but is not a []int.
func (vo Data) IntArray(name string) mo.Option[[]int] {
	return Get[[]int](vo, name)
}

// MstIntArray returns the []int value for the given name.
// It panics if the key is not found or the value is not a []int.
func (vo Data) MstIntArray(name string) []int {
	return vo.IntArray(name).MustGet()
}

// Int64Array returns an Option containing the []int64 value for the given name.
// It panics if the field exists but is not a []int64.
func (vo Data) Int64Array(name string) mo.Option[[]int64] {
	return Get[[]int64](vo, name)
}

// MstInt64Array returns the []int64 value for the given name.
// It panics if the key is not found or the value is not a []int64.
func (vo Data) MstInt64Array(name string) []int64 {
	return vo.Int64Array(name).MustGet()
}

// Float64Array returns an Option containing the []float64 value for the given name.
// It panics if the field exists but is not a []float64.
func (vo Data) Float64Array(name string) mo.Option[[]float64] {
	return Get[[]float64](vo, name)
}

// MstFloat64Array returns the []float64 value for the given name.
// It panics if the key is not found or the value is not a []float64.
func (vo Data) MstFloat64Array(name string) []float64 {
	return vo.Float64Array(name).MustGet()
}

// BoolArray returns an Option containing the []bool value for the given name.
// It panics if the field exists but is not a []bool.
func (vo Data) BoolArray(name string) mo.Option[[]bool] {
	return Get[[]bool](vo, name)
}

// MstBoolArray returns the []bool value for the given name.
// It panics if the key is not found or the value is not a []bool.
func (vo Data) MstBoolArray(name string) []bool {
	return vo.BoolArray(name).MustGet()
}
