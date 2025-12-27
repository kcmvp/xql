package xql

import (
	"errors"
	"fmt"
	"net/mail"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/samber/mo"
	"github.com/tidwall/match"

	"github.com/samber/lo"
)

type charSet int

type Validator[T FieldType] func(v T) error
type ValidateFunc[T FieldType] func() (string, Validator[T])

const (
	LowerCaseChar charSet = iota
	UpperCaseChar
	NumberChar
	SpecialChar
)

var (
	LowerCaseCharSet = string(lo.LowerCaseLettersCharset)
	UpperCaseCharSet = string(lo.UpperCaseLettersCharset)
	NumberCharSet    = string(lo.NumbersCharset)
	SpecialCharSet   = string(lo.SpecialCharset)
)

var (
	ErrIntegerOverflow = errors.New("integer overflow")
	ErrTypeMismatch    = errors.New("type mismatch")
	ErrRequired        = errors.New("is required but not found")

	ErrLengthMin     = errors.New("length must be at least")
	ErrLengthMax     = errors.New("length must be at most")
	ErrLengthExact   = errors.New("length must be exactly")
	ErrLengthBetween = errors.New("length must be between")

	ErrCharSetOnly      = errors.New("can only contain characters from")
	ErrCharSetAny       = errors.New("must contain at least one character from")
	ErrCharSetAll       = errors.New("not contains chars from")
	ErrCharSetNo        = errors.New("must not contain any characters from")
	ErrNotMatch         = errors.New("not match pattern")
	ErrNotValidEmail    = errors.New("not valid email address")
	ErrNotValidURL      = errors.New("not valid url")
	ErrNotOneOf         = errors.New("value must be one of")
	ErrMustGt           = errors.New("must be greater than")
	ErrMustGte          = errors.New("must be greater than or equal to")
	ErrMustLt           = errors.New("must be less than")
	ErrMustLte          = errors.New("must be less than or equal to")
	ErrMustBetween      = errors.New("must be between")
	ErrMustBeTrue       = errors.New("must be true")
	ErrMustBeFalse      = errors.New("must be false")
	ErrDecimalPrecision = errors.New("invalid decimal precision/scale")
)

// value is a private helper to get the character set and its descriptive name.
func (set charSet) value() (chars string, name string) {
	switch set {
	case LowerCaseChar:
		return LowerCaseCharSet, "lower case characters"
	case UpperCaseChar:
		return UpperCaseCharSet, "upper case characters"
	case NumberChar:
		return NumberCharSet, "numbers"
	case SpecialChar:
		return SpecialCharSet, "special characters"
	default:
		panic("unhandled default case in charSet.value()")
	}
}

// --- Clause Validators ---

// MinLength validates that a string's length is at least the specified minimum.
func MinLength(min int) ValidateFunc[string] {
	return func() (string, Validator[string]) {
		return "min_length", func(str string) error {
			return lo.Ternary(len(str) < min, fmt.Errorf("%w %d ", ErrLengthMin, min), nil)
		}
	}
}

// MaxLength validates that a string's length is at most the specified maximum.
func MaxLength(max int) ValidateFunc[string] {
	return func() (string, Validator[string]) {
		return "max_length", func(str string) error {
			return lo.Ternary(len(str) > max, fmt.Errorf("%w %d ", ErrLengthMax, max), nil)
		}
	}
}

// ExactLength validates that a string's length is exactly the specified length.
func ExactLength(length int) ValidateFunc[string] {
	return func() (string, Validator[string]) {
		return "exact_length", func(str string) error {
			return lo.Ternary(len(str) != length, fmt.Errorf("%w %d characters", ErrLengthExact, length), nil)
		}
	}

}

// LengthBetween validates that a string's length is within a given range (inclusive).
func LengthBetween(min, max int) ValidateFunc[string] {
	return func() (string, Validator[string]) {
		return "length_between", func(str string) error {
			length := len(str)
			return lo.Ternary(length < min || length > max, fmt.Errorf("%w %d and %d characters", ErrLengthBetween, min, max), nil)
		}
	}
}

// CharSetOnly validates that a string only contains characters from the specified character sets.
func CharSetOnly(charSets ...charSet) ValidateFunc[string] {
	return func() (string, Validator[string]) {
		return "only_contains", func(str string) error {
			var allChars strings.Builder
			var names []string
			for _, set := range charSets {
				chars, name := set.value()
				allChars.WriteString(chars)
				names = append(names, name)
			}
			for _, r := range str {
				if !strings.ContainsRune(allChars.String(), r) {
					return fmt.Errorf("%w: %s", ErrCharSetOnly, strings.Join(names, ", "))
				}
			}
			return nil
		}
	}
}

// CharSetAny validates that a string contains at least one character from any of the specified character sets.
func CharSetAny(charSets ...charSet) ValidateFunc[string] {
	return func() (string, Validator[string]) {
		return "contains_any", func(str string) error {
			var allChars strings.Builder
			var names []string
			for _, set := range charSets {
				chars, name := set.value()
				allChars.WriteString(chars)
				names = append(names, name)
			}
			return lo.Ternary(!strings.ContainsAny(allChars.String(), str), fmt.Errorf("%w: %s", ErrCharSetAny, strings.Join(names, ", ")), nil)
		}
	}
}

// CharSetAll validates that a string contains at least one character from each of the specified character sets.
func CharSetAll(charSets ...charSet) ValidateFunc[string] {
	return func() (string, Validator[string]) {
		return "contains_all", func(str string) error {
			for _, set := range charSets {
				chars, name := set.value()
				if !strings.ContainsAny(chars, str) {
					return fmt.Errorf("%w: %s", ErrCharSetAll, name)
				}
			}
			return nil
		}
	}

}

// CharSetNo validates that a string does not contain any characters from the specified character sets.
func CharSetNo(charSets ...charSet) ValidateFunc[string] {
	return func() (string, Validator[string]) {
		return "not_contains", func(str string) error {
			for _, set := range charSets {
				chars, name := set.value()
				if strings.ContainsAny(str, chars) {
					return fmt.Errorf("%s: %s", ErrCharSetNo, name)
				}
			}
			return nil
		}
	}
}

// Match validates that a string matches a given pattern.
// The pattern can include wildcards:
//   - `*`: matches any sequence of non-separator characters.
//   - `?`: matches any single non-separator character.
//
// Example: Match("foo*") will match "foobar", "foo", etc.
func Match(pattern string) ValidateFunc[string] {
	lo.Assertf(match.IsPattern(pattern), "invalid pattern `%s`: `?` stands for one character, `*` stands for any number of characters", pattern)
	return func() (string, Validator[string]) {
		return "match", func(str string) error {
			return lo.Ternary(!match.Match(str, pattern), fmt.Errorf("%w %s", ErrNotMatch, pattern), nil)
		}
	}
}

// Email validates that a string is a valid email address.
func Email() ValidateFunc[string] {
	return func() (string, Validator[string]) {
		return "email", func(str string) error {
			return lo.Ternary(mo.TupleToResult[*mail.Address](mail.ParseAddress(str)).IsError(), fmt.Errorf("%w:%s", ErrNotValidEmail, str), nil)
		}
	}
}

// URL validates that a string is a valid URL.
func URL() ValidateFunc[string] {
	return func() (string, Validator[string]) {
		return "url", func(str string) error {
			rs := mo.TupleToResult[*url.URL](url.Parse(str))
			errRs := rs.IsError() || rs.MustGet().Scheme == "" || rs.MustGet().Host == ""
			return lo.Ternary(errRs, fmt.Errorf("%w: %s", ErrNotValidURL, str), nil)
		}
	}
}

// --- Generic and Comparison types.Validators ---

// OneOf validates that a value is one of the allowed values.
// This works for any comparable type in FieldType (string, bool, all numbers).
func OneOf[T FieldType](allowed ...T) ValidateFunc[T] {
	return func() (string, Validator[T]) {
		return "one_of", func(val T) error {
			return lo.Ternary(!lo.Contains(allowed, val), fmt.Errorf("%w:%v", ErrNotOneOf, allowed), nil)
		}
	}
}

// Gt validates that a value is greater than the specified minimum.
func Gt[T Number | time.Time](min T) ValidateFunc[T] {
	return func() (string, Validator[T]) {
		return "gt", func(val T) error {
			return lo.Ternary(!isGreaterThan(val, min), fmt.Errorf("%w %v", ErrMustGt, min), nil)
		}
	}
}

// Gte validates that a value is greater than or equal to the specified minimum.
func Gte[T Number | time.Time](min T) ValidateFunc[T] {
	return func() (string, Validator[T]) {
		return "gte", func(val T) error {
			return lo.Ternary(isLessThan(val, min), fmt.Errorf("%w %v", ErrMustGte, min), nil)
		}
	}
}

// Lt validates that a value is less than the specified maximum.
func Lt[T Number | time.Time](max T) ValidateFunc[T] {
	return func() (string, Validator[T]) {
		return "lt", func(val T) error {
			return lo.Ternary(!isLessThan(val, max), fmt.Errorf("%w %v", ErrMustLt, max), nil)
		}
	}
}

// Lte validates that a value is less than or equal to the specified maximum.
func Lte[T Number | time.Time](max T) ValidateFunc[T] {
	return func() (string, Validator[T]) {
		return "lte", func(val T) error {
			return lo.Ternary(isGreaterThan(val, max), fmt.Errorf("%w %v", ErrMustLte, max), nil)
		}
	}
}

// Between validates that a value is within a given range (inclusive of min and max).
func Between[T Number | time.Time](min, max T) ValidateFunc[T] {
	return func() (string, Validator[T]) {
		return "between", func(val T) error {
			return lo.Ternary(isLessThan(val, min) || isGreaterThan(val, max), fmt.Errorf("%w %v and %v", ErrMustBetween, min, max), nil)
		}
	}
}

// --- Boolean Validators ---

// BeTrue validates that a boolean value is true.
func BeTrue() ValidateFunc[bool] {
	return func() (string, Validator[bool]) {
		return "be_true", func(b bool) error {
			return lo.Ternary(!b, ErrMustBeTrue, nil)
		}
	}
}

// BeFalse validates that a boolean value is false.
func BeFalse() ValidateFunc[bool] {
	return func() (string, Validator[bool]) {
		return "be_false", func(b bool) error {
			return lo.Ternary(b, ErrMustBeFalse, nil)
		}
	}
}

// isGreaterThan is a helper function that compares two values of type Number or time.Time
// and returns true if 'a' is strictly greater than 'b'.
// It handles different numeric types and time.Time by type assertion.
func isGreaterThan[T Number | time.Time](a, b T) bool {
	switch v := any(a).(type) {
	case time.Time:
		return v.After(any(b).(time.Time))
	case int:
		return v > any(b).(int)
	case int8:
		return v > any(b).(int8)
	case int16:
		return v > any(b).(int16)
	case int32:
		return v > any(b).(int32)
	case int64:
		return v > any(b).(int64)
	case uint:
		return v > any(b).(uint)
	case uint8:
		return v > any(b).(uint8)
	case uint16:
		return v > any(b).(uint16)
	case uint32:
		return v > any(b).(uint32)
	case uint64:
		return v > any(b).(uint64)
	case float32:
		return v > any(b).(float32)
	case float64:
		return v > any(b).(float64)
	}
	return false
}

// isLessThan is a helper function that compares two values of type Number or time.Time
// and returns true if 'a' is strictly less than 'b'.
// It handles different numeric types and time.Time by type assertion.

func isLessThan[T Number | time.Time](a, b T) bool {
	switch v := any(a).(type) {
	case time.Time:
		return v.Before(any(b).(time.Time))
	case int:
		return v < any(b).(int)
	case int8:
		return v < any(b).(int8)
	case int16:
		return v < any(b).(int16)
	case int32:
		return v < any(b).(int32)
	case int64:
		return v < any(b).(int64)
	case uint:
		return v < any(b).(uint)
	case uint8:
		return v < any(b).(uint8)
	case uint16:
		return v < any(b).(uint16)
	case uint32:
		return v < any(b).(uint32)
	case uint64:
		return v < any(b).(uint64)
	case float32:
		return v < any(b).(float32)
	case float64:
		return v < any(b).(float64)
	}
	return false
}

// DefaultTimeLayouts are the default layouts used to parse time strings.
var DefaultTimeLayouts = []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"}

// Decimal validates that a string representation of a decimal number conforms to
// the specified precision (total digits) and scale (fractional digits).
// Behavior:
//   - Empty string is considered valid (presence is handled elsewhere).
//   - Accepts optional leading '+' or '-' sign.
//   - Does not accept scientific notation (e.g., 1e3).
//   - Counts digits from integer and fractional parts; total digits must be <= precision
//     and fractional digits must be <= scale.
func Decimal(precision, scale int) ValidateFunc[string] {
	return func() (string, Validator[string]) {
		name := fmt.Sprintf("decimal(%d,%d)", precision, scale)
		return name, func(s string) error {
			s = strings.TrimSpace(s)
			if s == "" {
				return nil
			}
			// optional sign
			if s[0] == '+' || s[0] == '-' {
				s = s[1:]
			}
			// disallow scientific notation
			if strings.ContainsAny(s, "eE") {
				return fmt.Errorf("%w: unsupported format", ErrDecimalPrecision)
			}
			parts := strings.SplitN(s, ".", 3)
			if len(parts) > 2 {
				return fmt.Errorf("%w: invalid format", ErrDecimalPrecision)
			}
			intPart := parts[0]
			fracPart := ""
			if len(parts) == 2 {
				fracPart = parts[1]
			}
			// allow leading dot like `.12` -> intPart == ""
			if intPart == "" {
				intPart = "0"
			}
			if !allDigits(intPart) || !allDigits(fracPart) {
				return fmt.Errorf("%w: contains non-digit characters", ErrDecimalPrecision)
			}
			totalDigits := len(intPart) + len(fracPart)
			if totalDigits > precision || len(fracPart) > scale {
				return fmt.Errorf("%w %d,%d", ErrDecimalPrecision, precision, scale)
			}
			return nil
		}
	}
}

// allDigits returns true iff s consists only of ASCII digits (0-9).
func allDigits(s string) bool {
	if s == "" {
		return true
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ParseStringTo converts a string into the specified FieldType T.
// This is shared by view/value layers when converting URL params into typed values.
func ParseStringTo[T FieldType](s string) (T, error) {
	var zero T
	targetType := reflect.TypeOf(zero)

	switch targetType.Kind() {
	case reflect.String:
		return any(s).(T), nil
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return zero, fmt.Errorf("could not parse '%s' as bool: %w", s, err)
		}
		return any(b).(T), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return zero, fmt.Errorf("could not parse '%s' as int: %w", s, err)
		}
		if reflect.New(targetType).Elem().OverflowInt(val) {
			return zero, OverflowError(zero)
		}
		return reflect.ValueOf(val).Convert(targetType).Interface().(T), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		val, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return zero, fmt.Errorf("could not parse '%s' as uint: %w", s, err)
		}
		if reflect.New(targetType).Elem().OverflowUint(val) {
			return zero, OverflowError(zero)
		}
		return reflect.ValueOf(val).Convert(targetType).Interface().(T), nil
	case reflect.Float32, reflect.Float64:
		val, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return zero, fmt.Errorf("could not parse '%s' as float: %w", s, err)
		}
		if reflect.New(targetType).Elem().OverflowFloat(val) {
			return zero, fmt.Errorf("value %f overflows type %T", val, zero)
		}
		return reflect.ValueOf(val).Convert(targetType).Interface().(T), nil
	case reflect.Struct:
		if targetType == reflect.TypeOf(time.Time{}) {
			for _, layout := range DefaultTimeLayouts {
				if t, err := time.Parse(layout, s); err == nil {
					return any(t).(T), nil
				}
			}
			return zero, fmt.Errorf("incorrect date format for string '%s'", s)
		}
		fallthrough
	default:
		return zero, fmt.Errorf("type mismatch or unsupported type %T", zero)
	}
}

// OverflowError returns a standard overflow error wrapping ErrIntegerOverflow.
func OverflowError[T any](v T) error {
	return fmt.Errorf("for type %T: %w", v, ErrIntegerOverflow)
}
