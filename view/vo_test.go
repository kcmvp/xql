package view

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kcmvp/xql"
	"github.com/kcmvp/xql/validator"
	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// zero returns the zero value of a generic type T — used in tests to produce a
// value whose dynamic type can be inspected without writing type-specific
// literals like bool(false).
func zero[T any]() T { var z T; return z }

func TestTypedString(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name                string
		input               string
		targetType          any
		want                any
		expectedErr         error
		expectedErrContains string
	}{
		// Clause cases
		{
			name:       "string_ok",
			input:      "hello world",
			targetType: "",
			want:       "hello world",
		},
		// Bool cases
		{
			name:       "bool_true_ok",
			input:      "true",
			targetType: zero[bool](),
			want:       true,
		},
		{
			name:       "bool_false_ok",
			input:      "false",
			targetType: zero[bool](),
			want:       false,
		},
		{
			name:       "bool_1_ok",
			input:      "1",
			targetType: zero[bool](),
			want:       true,
		},
		{
			name:       "bool_0_ok",
			input:      "0",
			targetType: zero[bool](),
			want:       false,
		},
		{
			name:                "bool_invalid_string",
			input:               "not-a-bool",
			targetType:          zero[bool](),
			expectedErrContains: "could not parse 'not-a-bool' as bool",
		},
		// Integer cases
		{
			name:       "int_ok",
			input:      "123",
			targetType: int(0),
			want:       int(123),
		},
		{
			name:       "int8_ok",
			input:      "-128",
			targetType: int8(0),
			want:       int8(-128),
		},
		{
			name:        "int8_overflow",
			input:       "128",
			targetType:  int8(0),
			expectedErr: validator.ErrIntegerOverflow,
		},
		{
			name:                "int_invalid_format",
			input:               "not-a-number",
			targetType:          int(0),
			expectedErrContains: "could not parse 'not-a-number' as int",
		},
		{
			name:                "int_from_float_string",
			input:               "123.45",
			targetType:          int(0),
			expectedErrContains: "could not parse '123.45' as int",
		},
		// Unsigned Integer cases
		{
			name:       "uint_ok",
			input:      "123",
			targetType: uint(0),
			want:       uint(123),
		},
		{
			name:       "uint8_ok",
			input:      "255",
			targetType: uint8(0),
			want:       uint8(255),
		},
		{
			name:        "uint8_overflow",
			input:       "256",
			targetType:  uint8(0),
			expectedErr: validator.ErrIntegerOverflow,
		},
		{
			name:                "uint_negative_fail",
			input:               "-1",
			targetType:          uint(0),
			expectedErrContains: "could not parse '-1' as uint",
		},
		// Float cases
		{
			name:       "float64_ok",
			input:      "123.45",
			targetType: float64(0),
			want:       float64(123.45),
		},
		{
			name:       "float32_ok",
			input:      "1.2e3",
			targetType: float32(0),
			want:       float32(1200),
		},
		{
			name:                "float32_overflow",
			input:               "3.5e38",
			targetType:          float32(0),
			expectedErrContains: "overflows type",
		},
		// Time cases
		{
			name:       "time_rfc3339_ok",
			input:      now.Format(time.RFC3339Nano),
			targetType: time.Time{},
			want:       now,
		},
		{
			name:       "time_date_only_ok",
			input:      "2024-01-15",
			targetType: time.Time{},
			want:       time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:                "time_invalid_format",
			input:               "15-01-2024",
			targetType:          time.Time{},
			expectedErrContains: "incorrect date format",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got mo.Result[any]
			switch tc.targetType.(type) {
			case string:
				got = mo.TupleToResult[any](typedString[string](tc.input).Get())
			case bool:
				got = mo.TupleToResult[any](typedString[bool](tc.input).Get())
			case int:
				got = mo.TupleToResult[any](typedString[int](tc.input).Get())
			case int8:
				got = mo.TupleToResult[any](typedString[int8](tc.input).Get())
			case uint:
				got = mo.TupleToResult[any](typedString[uint](tc.input).Get())
			case uint8:
				got = mo.TupleToResult[any](typedString[uint8](tc.input).Get())
			case float32:
				got = mo.TupleToResult[any](typedString[float32](tc.input).Get())
			case float64:
				got = mo.TupleToResult[any](typedString[float64](tc.input).Get())
			case time.Time:
				got = mo.TupleToResult[any](typedString[time.Time](tc.input).Get())
			default:
				t.Fatalf("unhandled test type: %T", tc.targetType)
			}

			if tc.expectedErr != nil {
				require.True(t, got.IsError(), "expected an error but got none")
				require.ErrorIs(t, got.Error(), tc.expectedErr, "did not get expected error type")
			} else if tc.expectedErrContains != "" {
				require.True(t, got.IsError(), "expected an error but got none")
				require.Contains(t, got.Error().Error(), tc.expectedErrContains, "error message does not contain expected text")
			} else {
				require.False(t, got.IsError(), "got unexpected error: %v", got.Error())
				if wantTime, ok := tc.want.(time.Time); ok {
					gotTime := got.MustGet().(time.Time)
					require.WithinDuration(t, wantTime, gotTime, time.Second)
				} else {
					require.Equal(t, tc.want, got.MustGet())
				}
			}
		})
	}
}

func TestJSONField_validateRaw(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name                string
		field               ViewField
		input               string
		wantValue           any
		wantErr             error
		expectedErrContains string
	}{
		{
			name:      "string_success_with_validator",
			field:     Field[string]("name", validator.MinLength(3)),
			input:     "gopher",
			wantValue: "gopher",
		},
		{
			name:    "string_validation_fail",
			field:   Field[string]("name", validator.MinLength(10)),
			input:   "gopher",
			wantErr: validator.ErrLengthMin,
		},
		{
			name:      "int_success_with_validator",
			field:     Field[int]("age", validator.Gt(18)),
			input:     "20",
			wantValue: 20,
		},
		{
			name:    "int_validation_fail",
			field:   Field[int]("age", validator.Gt(18)),
			input:   "18",
			wantErr: validator.ErrMustGt,
		},
		{
			name:                "int_parsing_fail",
			field:               Field[int]("age"),
			input:               "not-an-age",
			expectedErrContains: "could not parse 'not-an-age' as int",
		},
		{
			name:      "bool_success",
			field:     Field[bool]("active"),
			input:     "true",
			wantValue: true,
		},
		{
			name:                "bool_parsing_fail",
			field:               Field[bool]("active"),
			input:               "yes",
			expectedErrContains: "could not parse 'yes' as bool",
		},
		{
			name:      "time_success",
			field:     Field[time.Time]("createdAt"),
			input:     now.Format(time.RFC3339Nano),
			wantValue: now,
		},
		{
			name:                "time_parsing_fail",
			field:               Field[time.Time]("createdAt"),
			input:               "not-a-time",
			expectedErrContains: "incorrect date format",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs := tc.field.validateRaw(tc.input)
			if tc.wantErr != nil {
				require.True(t, rs.IsError(), "expected an error but got none")
				require.ErrorIs(t, rs.Error(), tc.wantErr, "did not get expected error type")
			} else if tc.expectedErrContains != "" {
				require.True(t, rs.IsError(), "expected an error but got none")
				require.Contains(t, rs.Error().Error(), tc.expectedErrContains, "error message does not contain expected text")
			} else {
				require.False(t, rs.IsError(), "got unexpected error: %v", rs.Error())
				if wantTime, ok := tc.wantValue.(time.Time); ok {
					gotTime := rs.MustGet().(time.Time)
					// Use WithinDuration for time comparison to handle minor precision differences
					require.WithinDuration(t, wantTime, gotTime, time.Second)
				} else {
					require.Equal(t, tc.wantValue, rs.MustGet())
				}
			}
		})
	}
}

func TestTyped(t *testing.T) {
	t.Run("integers", func(t *testing.T) {
		// Test cases for integer types
		tests := []struct {
			name                string
			json                string
			want                mo.Result[any]
			targetType          any
			expectedErr         error
			expectedErrContains string
		}{
			{
				name:       "int_ok",
				json:       `{"value": 123}`,
				want:       mo.Ok(any(int(123))),
				targetType: int(0),
			},
			{
				name:        "int_from_string_fail",
				json:        `{"value": "123"}`,
				targetType:  int(0),
				expectedErr: validator.ErrTypeMismatch,
			},
			{
				name:        "int_overflow",
				json:        fmt.Sprintf(`{"value": %d1}`, math.MaxInt), // Overflow
				targetType:  int(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:       "int8_ok",
				json:       `{"value": 127}`,
				want:       mo.Ok(any(int8(127))),
				targetType: int8(0),
			},
			{
				name:        "int8_overflow",
				json:        `{"value": 128}`,
				targetType:  int8(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:       "int16_ok",
				json:       `{"value": 32767}`,
				want:       mo.Ok(any(int16(32767))),
				targetType: int16(0),
			},
			{
				name:        "int16_overflow",
				json:        `{"value": 32768}`,
				targetType:  int16(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:       "int32_ok",
				json:       `{"value": 2147483647}`,
				want:       mo.Ok(any(int32(2147483647))),
				targetType: int32(0),
			},
			{
				name:        "int32_overflow",
				json:        `{"value": 2147483648}`,
				targetType:  int32(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:       "int64_ok",
				json:       fmt.Sprintf(`{"value": %d}`, int64(math.MaxInt64)),
				want:       mo.Ok(any(int64(math.MaxInt64))),
				targetType: int64(0),
			},
			{
				name:        "int64_overflow",
				json:        `{"value": 9223372036854775808}`,
				targetType:  int64(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:        "int64_underflow",
				json:        `{"value": -9223372036854775809}`,
				targetType:  int64(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:        "int_from_float_fail",
				json:        `{"value": 123.45}`,
				targetType:  int(0),
				expectedErr: validator.ErrTypeMismatch,
			},
			{
				name:        "int_from_bool_fail",
				json:        `{"value": true}`,
				targetType:  int(0),
				expectedErr: validator.ErrTypeMismatch,
			},
			{
				name:        "int_from_malformed_float_fail",
				json:        `{"value": 1.2.3}`, // gjson is lenient and parses this as a number
				targetType:  int(0),
				expectedErr: validator.ErrTypeMismatch,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				res := gjson.Get(tc.json, "value")
				var got mo.Result[any]
				switch tc.targetType.(type) {
				case int:
					got = mo.TupleToResult[any](typedJson[int](res).Get())
				case int8:
					got = mo.TupleToResult[any](typedJson[int8](res).Get())
				case int16:
					got = mo.TupleToResult[any](typedJson[int16](res).Get())
				case int32:
					got = mo.TupleToResult[any](typedJson[int32](res).Get())
				case int64:
					got = mo.TupleToResult[any](typedJson[int64](res).Get())
				default:
					t.Fatalf("unhandled test type: %T", tc.targetType)
				}
				if tc.expectedErr != nil {
					require.True(t, got.IsError(), "expected an error but got none")
					require.ErrorIs(t, got.Error(), tc.expectedErr, "did not get expected error type")
				} else if tc.expectedErrContains != "" {
					require.True(t, got.IsError(), "expected an error but got none")
					require.Contains(t, got.Error().Error(), tc.expectedErrContains, "error message does not contain expected text")
				} else {
					require.False(t, got.IsError(), "got unexpected error: %v", got.Error())
					require.Equal(t, tc.want.MustGet(), got.MustGet())
				}
			})
		}
	})
	t.Run("unsigned integers", func(t *testing.T) {
		tests := []struct {
			name                string
			json                string
			want                mo.Result[any]
			targetType          any
			expectedErr         error
			expectedErrContains string
		}{
			{
				name:       "uint_ok",
				json:       `{"value": 123}`,
				want:       mo.Ok(any(uint(123))),
				targetType: uint(0),
			},
			{
				name:        "uint_from_string_fail",
				json:        `{"value": "123"}`,
				targetType:  uint(0),
				expectedErr: validator.ErrTypeMismatch,
			},
			{
				name:        "uint_negative_fail",
				json:        `{"value": -1}`,
				targetType:  uint(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:       "uint8_ok",
				json:       `{"value": 255}`,
				want:       mo.Ok(any(uint8(255))),
				targetType: uint8(0),
			},
			{
				name:        "uint8_overflow",
				json:        `{"value": 256}`,
				targetType:  uint8(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:       "uint16_ok",
				json:       `{"value": 65535}`,
				want:       mo.Ok(any(uint16(65535))),
				targetType: uint16(0),
			},
			{
				name:        "uint16_overflow",
				json:        `{"value": 65536}`,
				targetType:  uint16(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:       "uint32_ok",
				json:       `{"value": 4294967295}`,
				want:       mo.Ok(any(uint32(4294967295))),
				targetType: uint32(0),
			},
			{
				name:        "uint32_overflow",
				json:        `{"value": 4294967296}`,
				targetType:  uint32(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:       "uint64_ok",
				json:       fmt.Sprintf(`{"value": %d}`, uint64(math.MaxUint64)),
				want:       mo.Ok(any(uint64(math.MaxUint64))),
				targetType: uint64(0),
			},
			{
				name:        "uint64_overflow",
				json:        fmt.Sprintf(`{"value":%s}`, "18446744073709551616"), // MaxUint64 + 1
				targetType:  uint64(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:        "uint64_overflow_from_large_number",
				json:        `{"value": 18446744073709551616}`,
				targetType:  uint64(0),
				expectedErr: validator.ErrIntegerOverflow,
			},
			{
				name:        "uint_from_float_fail",
				json:        `{"value": 123.45}`,
				targetType:  uint(0),
				expectedErr: validator.ErrTypeMismatch,
			},
			{
				name:        "uint_from_bool_fail",
				json:        `{"value": true}`,
				targetType:  uint(0),
				expectedErr: validator.ErrTypeMismatch,
			},
			{
				name:        "uint_from_malformed_float_fail",
				json:        `{"value": 1.2.3}`, // gjson is lenient and parses this as a number
				targetType:  uint(0),
				expectedErr: validator.ErrTypeMismatch,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				res := gjson.Get(tc.json, "value")
				var got mo.Result[any]
				switch tc.targetType.(type) {
				case uint:
					got = mo.TupleToResult[any](typedJson[uint](res).Get())
				case uint8:
					got = mo.TupleToResult[any](typedJson[uint8](res).Get())
				case uint16:
					got = mo.TupleToResult[any](typedJson[uint16](res).Get())
				case uint32:
					got = mo.TupleToResult[any](typedJson[uint32](res).Get())
				case uint64:
					got = mo.TupleToResult[any](typedJson[uint64](res).Get())
				default:
					t.Fatalf("unhandled test type: %T", tc.targetType)
				}

				if tc.expectedErr != nil {
					require.True(t, got.IsError(), "expected an error but got none")
					require.ErrorIs(t, got.Error(), tc.expectedErr, "did not get expected error type")
				} else if tc.expectedErrContains != "" {
					require.True(t, got.IsError(), "expected an error but got none")
					require.Contains(t, got.Error().Error(), tc.expectedErrContains, "error message does not contain expected text")
				} else {
					require.False(t, got.IsError(), "got unexpected error: %v", got.Error())
					require.Equal(t, tc.want.MustGet(), got.MustGet())
				}
			})
		}
	})
	t.Run("floats", func(t *testing.T) {
		tests := []struct {
			name        string
			json        string
			want        mo.Result[any]
			targetType  any
			expectedErr bool
		}{
			{
				name:       "float32_ok",
				json:       `{"value": 123.45}`,
				want:       mo.Ok(any(float32(123.45))),
				targetType: float32(0),
			},
			{
				name:        "float32_overflow",
				json:        `{"value": 3.5e+38}`,
				targetType:  float32(0),
				expectedErr: true,
			},
			{
				name:       "float64_ok",
				json:       `{"value": 1.7976931348623157e+308}`,
				want:       mo.Ok(any(float64(1.7976931348623157e+308))),
				targetType: float64(0),
			},
			{
				name:        "float64_overflow",
				json:        `{"value": 1.8e+308}`,
				want:        mo.Ok(any(math.Inf(1))),
				targetType:  float64(0),
				expectedErr: false,
			},
			{
				name:       "float_from_string_fail",
				json:       `{"value": "123.45"}`,
				want:       mo.Ok(any(123.45)),
				targetType: float64(0),
				//expectedErr: true,
			},
			{
				name:        "float_from_bool_fail",
				json:        `{"value": true}`,
				targetType:  float64(0),
				expectedErr: true,
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				res := gjson.Get(tc.json, "value")
				var got mo.Result[any]
				switch tc.targetType.(type) {
				case float32:
					got = mo.TupleToResult[any](typedJson[float32](res).Get())
				case float64:
					got = mo.TupleToResult[any](typedJson[float64](res).Get())
				default:
					t.Fatalf("unhandled test type: %T", tc.targetType)
				}

				if tc.expectedErr {
					require.True(t, got.IsError(), "expected an error but got none")
				} else {
					require.False(t, got.IsError(), "got unexpected error: %v", got.Error())
					gotVal := got.MustGet()
					wantVal := tc.want.MustGet()
					isInf := false
					if f, ok := wantVal.(float64); ok {
						if math.IsInf(f, 0) {
							isInf = true
						}
					} else if f, ok := wantVal.(float32); ok {
						if math.IsInf(float64(f), 0) {
							isInf = true
						}
					}
					if isInf {
						require.Equal(t, wantVal, gotVal)
					} else {
						require.InDelta(t, wantVal, gotVal, 1e-9)
					}
				}
			})
		}
	})
	t.Run("booleans", func(t *testing.T) {
		tests := []struct {
			name        string
			json        string
			want        mo.Result[any]
			targetType  any
			expectedErr error
		}{
			{
				name:       "bool_ok",
				json:       `{"value": true}`,
				want:       mo.Ok(any(true)),
				targetType: zero[bool](),
			},
			{
				name:        "bool_from_string_fail",
				json:        `{"value": "true"}`,
				targetType:  zero[bool](),
				expectedErr: validator.ErrTypeMismatch,
			},
			{
				name:        "bool_from_number_fail",
				json:        `{"value": 1}`,
				targetType:  zero[bool](),
				expectedErr: validator.ErrTypeMismatch,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				res := gjson.Get(tc.json, "value")
				var got mo.Result[any]
				switch tc.targetType.(type) {
				case bool:
					got = mo.TupleToResult[any](typedJson[bool](res).Get())
				default:
					t.Fatalf("unhandled test type: %T", tc.targetType)
				}

				if tc.expectedErr != nil {
					require.True(t, got.IsError(), "expected an error but got none")
					require.ErrorIs(t, got.Error(), tc.expectedErr, "did not get expected error type")
				} else {
					require.False(t, got.IsError(), "got unexpected error: %v", got.Error())
					require.Equal(t, tc.want.MustGet(), got.MustGet())
				}
			})
		}
	})
	t.Run("time", func(t *testing.T) {
		// Test cases for time.Time
		now := time.Now()
		tests := []struct {
			name        string
			json        string
			want        mo.Result[time.Time]
			expectedErr bool
		}{
			{
				name: "time_ok_rfc3339",
				json: fmt.Sprintf(`{"value": "%s"}`, now.Format(time.RFC3339)),
				want: mo.Ok(now.Truncate(time.Second)),
			},
			{
				name: "time_ok_date_only",
				json: `{"value": "2023-01-15"}`,
				want: mo.Ok(time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)),
			},
			{
				name:        "time_invalid_format",
				json:        `{"value": "15-01-2023"}`,
				expectedErr: true,
			},
			{
				name:        "time_from_number_fail",
				json:        `{"value": 1234567890}`,
				expectedErr: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				res := gjson.Get(tc.json, "value")
				got := typedJson[time.Time](res)

				if tc.expectedErr {
					require.True(t, got.IsError(), "expected an error but got none")
				} else {
					require.False(t, got.IsError(), "got unexpected error: %v", got.Error())
					// Truncate to remove monotonic clock readings for comparison
					require.WithinDuration(t, tc.want.MustGet(), got.MustGet(), time.Second, "time values are not equal")
				}
			})
		}
	})
}

func TestValidationError_Error(t *testing.T) {
	tests := []struct {
		name  string
		err   *validationError
		check func(t *testing.T, got string)
	}{
		{
			name: "nil error",
			err:  nil,
			check: func(t *testing.T, got string) {
				require.Equal(t, "", got)
			},
		},
		{
			name: "empty error",
			err:  &validationError{},
			check: func(t *testing.T, got string) {
				require.Equal(t, "", got)
			},
		},
		{
			name: "one error",
			err: &validationError{
				errors: map[string]error{
					"field1": errors.New("error 1"),
				},
			},
			check: func(t *testing.T, got string) {
				require.Contains(t, got, "- field1: error 1")
			},
		},
		{
			name: "multiple errors",
			err: &validationError{
				errors: map[string]error{
					"field1": errors.New("error 1"),
					"field2": errors.New("error 2"),
				},
			},
			check: func(t *testing.T, got string) {
				require.True(t, strings.HasPrefix(got, "validation failed with the following errors:"))
				require.Contains(t, got, "- field1: error 1")
				require.Contains(t, got, "- field2: error 2")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			tt.check(t, got)
		})
	}
}

func TestValidationError_add(t *testing.T) {
	t.Run("add to nil error map", func(t *testing.T) {
		e := &validationError{}
		require.Nil(t, e.errors)
		err := errors.New("some error")
		e.add("field1", err)
		require.NotNil(t, e.errors)
		require.Equal(t, err, e.errors["field1"])
	})

	t.Run("add to existing error map", func(t *testing.T) {
		e := &validationError{
			errors: make(map[string]error),
		}
		err1 := errors.New("error 1")
		e.add("field1", err1)
		require.Equal(t, err1, e.errors["field1"])

		err2 := errors.New("error 2")
		e.add("field2", err2)
		require.Equal(t, err2, e.errors["field2"])
		require.Len(t, e.errors, 2)
	})

	t.Run("overwrite existing error", func(t *testing.T) {
		e := &validationError{
			errors: make(map[string]error),
		}
		err1 := errors.New("error 1")
		e.add("field1", err1)
		require.Equal(t, err1, e.errors["field1"])

		errOverwrite := errors.New("overwrite error")
		e.add("field1", errOverwrite)
		require.Equal(t, errOverwrite, e.errors["field1"])
		require.Len(t, e.errors, 1)
	})

	t.Run("add nil error", func(t *testing.T) {
		e := &validationError{}
		e.add("field1", nil)
		require.Nil(t, e.errors)
		require.Len(t, e.errors, 0)

		e.errors = make(map[string]error)
		e.add("field2", nil)
		require.Len(t, e.errors, 0)
	})
}

func TestValidationError_err(t *testing.T) {
	tests := []struct {
		name    string
		err     *validationError
		wantErr bool
	}{
		{
			name:    "nil validationError",
			err:     nil,
			wantErr: false,
		},
		{
			name:    "validationError with nil errors map",
			err:     &validationError{errors: nil},
			wantErr: false,
		},
		{
			name:    "validationError with empty errors map",
			err:     &validationError{errors: make(map[string]error)},
			wantErr: false,
		},
		{
			name: "validationError with one error",
			err: &validationError{
				errors: map[string]error{
					"field1": errors.New("error 1"),
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.err.err()
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, tt.err, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchemaField_validate(t *testing.T) {
	tests := []struct {
		name      string
		field     ViewField
		json      string
		wantValue any
		wantErr   error
	}{
		{
			name:      "required_field_present_and_valid",
			field:     Field[string]("name"),
			json:      `{"name": "gopher"}`,
			wantValue: "gopher",
			wantErr:   nil,
		},
		{
			name:      "required_field_present_but_invalid",
			field:     Field[string]("name", validator.MinLength(10)),
			json:      `{"name": "gopher"}`,
			wantValue: nil,
			wantErr:   validator.ErrLengthMin,
		},
		{
			name:      "optional_field_present_and_valid",
			field:     Field[string]("name").Optional(),
			json:      `{"name": "gopher"}`,
			wantValue: "gopher",
			wantErr:   nil,
		},
		{
			name:      "optional_field_present_but_invalid",
			field:     Field[string]("name", validator.MinLength(10)).Optional(),
			json:      `{"name": "gopher"}`,
			wantValue: nil,
			wantErr:   validator.ErrLengthMin,
		},
		{
			name:      "type_mismatch",
			field:     Field[int]("age"),
			json:      `{"age": "not-an-age"}`,
			wantValue: nil,
			wantErr:   validator.ErrTypeMismatch,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs := tc.field.validate(gjson.Get(tc.json, tc.field.Name()))
			if tc.wantErr != nil {
				require.Error(t, rs.Error())
				require.ErrorIs(t, rs.Error(), tc.wantErr)
			} else {
				require.NoError(t, rs.Error())
				require.Equal(t, tc.wantValue, rs.MustGet())
			}
		})
	}
}

func TestWithFields(t *testing.T) {
	tests := []struct {
		name        string
		fields      []ViewField
		shouldPanic bool
	}{
		{
			name:        "no fields",
			fields:      []ViewField{},
			shouldPanic: false,
		},
		{
			name:        "one field",
			fields:      []ViewField{Field[string]("name")},
			shouldPanic: false,
		},
		{
			name: "multiple unique fields",
			fields: []ViewField{
				Field[string]("name"),
				Field[int]("age"),
			},
			shouldPanic: false,
		},
		{
			name: "duplicate field name",
			fields: []ViewField{
				Field[string]("name"),
				Field[int]("name"),
			},
			shouldPanic: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.shouldPanic {
				require.Panics(t, func() {
					WithFields(tc.fields...)
				})
			} else {
				vo := WithFields(tc.fields...)
				require.NotNil(t, vo)
				require.Len(t, vo.fields, len(tc.fields))
			}
		})
	}
}

func TestSchema_AllowUnknownFields(t *testing.T) {
	tests := []struct {
		name          string
		vo            *Schema
		json          string
		allowUnknown  bool
		expectErr     bool
		errContains   string
		expectVal     string
		expectPresent bool
	}{
		{
			name:         "Default behavior: unknown fields not allowed, should error",
			vo:           WithFields(Field[string]("name")),
			json:         `{"name": "gopher", "extra": "field"}`,
			allowUnknown: false,
			expectErr:    true,
			errContains:  "unknown json field 'extra'",
		},
		{
			name:          "AllowUnknownFields enabled: unknown fields allowed, should not error",
			vo:            WithFields(Field[string]("name")),
			json:          `{"name": "gopher", "extra": "field"}`,
			allowUnknown:  true,
			expectErr:     false,
			expectVal:     "gopher",
			expectPresent: true,
		},
		{
			name:         "No unknown fields: should not error (default)",
			vo:           WithFields(Field[string]("name")),
			json:         `{"name": "gopher"}`,
			allowUnknown: false,
			expectErr:    false,
		},
		{
			name:         "No unknown fields: should not error (allowed)",
			vo:           WithFields(Field[string]("name")),
			json:         `{"name": "gopher"}`,
			allowUnknown: true,
			expectErr:    false,
		},
		{
			name:         "Corner case: empty raw, should not error",
			vo:           WithFields(Field[string]("name")),
			json:         `{}`,
			allowUnknown: false,
			expectErr:    true, // required field is missing
			errContains:  "name is required",
		},
		{
			name:         "Corner case: empty Schema, should not error",
			vo:           WithFields(),
			json:         `{"name": "gopher"}`,
			allowUnknown: true,
			expectErr:    false,
		},
		{
			name:         "Corner case: empty Schema, unknown fields disallowed, should error",
			vo:           WithFields(),
			json:         `{"name": "gopher"}`,
			allowUnknown: false,
			expectErr:    true,
			errContains:  "unknown json field 'name'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.allowUnknown {
				// Test that the method is chainable
				returnedVo := tc.vo.AllowUnknownFields()
				require.Same(t, tc.vo, returnedVo, "AllowUnknownFields should be chainable")
			}

			res := tc.vo.Validate(tc.json)

			if tc.expectErr {
				require.True(t, res.IsError(), "expected an error but got none")
				require.Contains(t, res.Error().Error(), tc.errContains, "error message does not contain expected text")
			} else {
				require.False(t, res.IsError(), "got unexpected error: %v", res.Error())
				if tc.expectPresent {
					val, ok := res.MustGet().String("name").Get()
					require.True(t, ok)
					require.Equal(t, tc.expectVal, val)
				}
			}
		})
	}
}

func TestEndToEnd(t *testing.T) {
	// Define the Schema with various field types and constraints
	userSchema := WithFields(
		Field[string]("name", validator.MinLength(3)),
		Field[int]("age", validator.Between(18, 120)),
		Field[string]("email", validator.Email()).Optional(),
		Field[bool]("isActive"),
		Field[time.Time]("createdAt").Optional(),
		Field[float64]("rating", validator.Between(0.0, 5.0)),
		Field[string]("department").Optional(),
		Field[string]("username", validator.CharSetOnly(validator.LowerCaseChar)),
		Field[string]("nickname"), // No validator for 'not contains substring' yet
		Field[string]("countryCode", validator.ExactLength(2)),
		Field[string]("homepage", validator.URL()).Optional(),
		Field[string]("status", validator.OneOf[string]("active", "inactive", "pending")),
		Field[string]("tags", validator.Match(`tag_*`)),
		Field[float64]("salary", validator.Gt(0.0)),
		// New fields for all raw types
		Field[int8]("level", validator.Between[int8](1, 100)),
		Field[int16]("score", validator.Gt[int16](0)),
		Field[int32]("views", validator.Gte[int32](0)),
		Field[int64]("balance", validator.Gte[int64](0)),
		Field[uint]("flags"),
		Field[uint8]("version"),
		Field[uint16]("build"),
		Field[uint32]("instanceId"),
		Field[uint64]("nonce"),
		Field[float32]("ratio", validator.Between[float32](0.0, 1.0)),
	)

	// Test cases
	tests := []struct {
		name    string
		isValid bool
		check   func(t *testing.T, vo ValueObject)
	}{
		{
			name:    "valid user",
			isValid: true,
			check: func(t *testing.T, vo ValueObject) {
				// Clause
				name, ok := vo.String("name").Get()
				require.True(t, ok)
				require.Equal(t, "John Doe", name)
				email, ok := vo.String("email").Get()
				require.True(t, ok)
				require.Equal(t, "john.doe@example.com", email)
				username, ok := vo.String("username").Get()
				require.True(t, ok)
				require.Equal(t, "johndoe", username)
				nickname, ok := vo.String("nickname").Get()
				require.True(t, ok)
				require.Equal(t, "Johnny", nickname)
				countryCode, ok := vo.String("countryCode").Get()
				require.True(t, ok)
				require.Equal(t, "US", countryCode)
				tags, ok := vo.String("tags").Get()
				require.True(t, ok)
				require.Equal(t, "tag_go,developer,testing", tags)
				status, ok := vo.String("status").Get()
				require.True(t, ok)
				require.Equal(t, "active", status)

				// Optional Clause not present
				_, ok = vo.String("department").Get()
				require.False(t, ok)

				// Bool
				isActive, ok := vo.Bool("isActive").Get()
				require.True(t, ok)
				require.True(t, isActive)

				// Time (optional, not present)
				_, ok = vo.Time("createdAt").Get()
				require.False(t, ok)

				// Numbers
				age, ok := vo.Int("age").Get()
				require.True(t, ok)
				require.Equal(t, 30, age)
				rating, ok := vo.Float64("rating").Get()
				require.True(t, ok)
				require.Equal(t, 4.5, rating)
				salary, ok := vo.Float64("salary").Get()
				require.True(t, ok)
				require.Equal(t, 50000.0, salary)
				level, ok := vo.Int8("level").Get()
				require.True(t, ok)
				require.Equal(t, int8(10), level)
				score, ok := vo.Int16("score").Get()
				require.True(t, ok)
				require.Equal(t, int16(1000), score)
				views, ok := vo.Int32("views").Get()
				require.True(t, ok)
				require.Equal(t, int32(100000), views)
				balance, ok := vo.Int64("balance").Get()
				require.True(t, ok)
				require.Equal(t, int64(1000000000), balance)
				flags, ok := vo.Uint("flags").Get()
				require.True(t, ok)
				require.Equal(t, uint(4294967295), flags)
				version, ok := vo.Uint8("version").Get()
				require.True(t, ok)
				require.Equal(t, uint8(255), version)
				build, ok := vo.Uint16("build").Get()
				require.True(t, ok)
				require.Equal(t, uint16(65535), build)
				instanceId, ok := vo.Uint32("instanceId").Get()
				require.True(t, ok)
				require.Equal(t, uint32(4294967295), instanceId)
				nonce, ok := vo.Uint64("nonce").Get()
				require.True(t, ok)
				require.Equal(t, uint64(18446744073709551615), nonce)
				ratio, ok := vo.Float32("ratio").Get()
				require.True(t, ok)
				require.Equal(t, float32(0.5), ratio)
			},
		},
		{
			name:    "invalid rating",
			isValid: false,
		},
		{
			name:    "missing required field",
			isValid: false,
		},
		{
			name:    "valid user with all optional fields",
			isValid: true,
			check: func(t *testing.T, vo ValueObject) {
				// Check optional fields that are present
				createdAt, ok := vo.Time("createdAt").Get()
				require.True(t, ok)
				expectedTime, _ := time.Parse(time.RFC3339, "2024-01-01T12:00:00Z")
				require.WithinDuration(t, expectedTime, createdAt, time.Second)

				department, ok := vo.String("department").Get()
				require.True(t, ok)
				require.Equal(t, "Security", department)

				homepage, ok := vo.String("homepage").Get()
				require.True(t, ok)
				require.Equal(t, "https://matrix.com", homepage)

				// Also check all other fields to ensure they are correctly parsed
				name, ok := vo.String("name").Get()
				require.True(t, ok)
				require.Equal(t, "John Doe", name)
				age, ok := vo.Int("age").Get()
				require.True(t, ok)
				require.Equal(t, 30, age)
				isActive, ok := vo.Bool("isActive").Get()
				require.True(t, ok)
				require.True(t, isActive)
				level, ok := vo.Int8("level").Get()
				require.True(t, ok)
				require.Equal(t, int8(10), level)
				score, ok := vo.Int16("score").Get()
				require.True(t, ok)
				require.Equal(t, int16(1000), score)
				views, ok := vo.Int32("views").Get()
				require.True(t, ok)
				require.Equal(t, int32(100000), views)
				balance, ok := vo.Int64("balance").Get()
				require.True(t, ok)
				require.Equal(t, int64(1000000000), balance)
				flags, ok := vo.Uint("flags").Get()
				require.True(t, ok)
				require.Equal(t, uint(4294967295), flags)
				version, ok := vo.Uint8("version").Get()
				require.True(t, ok)
				require.Equal(t, uint8(255), version)
				build, ok := vo.Uint16("build").Get()
				require.True(t, ok)
				require.Equal(t, uint16(65535), build)
				instanceId, ok := vo.Uint32("instanceId").Get()
				require.True(t, ok)
				require.Equal(t, uint32(4294967295), instanceId)
				nonce, ok := vo.Uint64("nonce").Get()
				require.True(t, ok)
				require.Equal(t, uint64(18446744073709551615), nonce)
				ratio, ok := vo.Float32("ratio").Get()
				require.True(t, ok)
				require.Equal(t, float32(0.5), ratio)
			},
		},
		{
			name:    "valid user without optional email",
			isValid: true,
			check: func(t *testing.T, vo ValueObject) {
				_, ok := vo.String("email").Get()
				require.False(t, ok, "email should not be present")

				// Check other fields to ensure they are still valid
				name, ok := vo.String("name").Get()
				require.True(t, ok)
				require.Equal(t, "John Doe", name)
				age, ok := vo.Int("age").Get()
				require.True(t, ok)
				require.Equal(t, 30, age)
				isActive, ok := vo.Bool("isActive").Get()
				require.True(t, ok)
				require.True(t, isActive)
			},
		},
		{
			name:    "invalid username charset",
			isValid: false,
		},
		{
			name:    "invalid countryCode length",
			isValid: false,
		},
		{
			name:    "invalid tags pattern",
			isValid: false,
		},
		{
			name:    "invalid homepage url",
			isValid: false,
		},
		{
			name:    "invalid status",
			isValid: false,
		},
		{
			name:    "invalid salary",
			isValid: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Read json from testdata
			jsonPath := filepath.Join("testdata", fmt.Sprintf("%s.json", strings.ReplaceAll(tc.name, " ", "_")))
			jsonData, err := os.ReadFile(jsonPath)
			require.NoError(t, err, "failed to read test data file")

			// validate
			res := userSchema.Validate(string(jsonData))

			if tc.isValid {
				require.False(t, res.IsError(), "expected validation to succeed, but it failed with: %v", res.Error())
				vo := res.MustGet()
				require.NotNil(t, vo)
				if tc.check != nil {
					tc.check(t, vo)
				}
			} else {
				require.True(t, res.IsError(), "expected validation to fail, but it succeeded")
			}
		})
	}
}

func TestValueObject_AddUpdate(t *testing.T) {
	t.Run("Add", func(t *testing.T) {
		vo := valueObject{
			Data: map[string]any{},
		}

		// Test successful Add
		require.NotPanics(t, func() {
			vo.Add("name", "gopher")
		})
		name, ok := vo.Get("name").Get()
		require.True(t, ok)
		require.Equal(t, "gopher", name)

		// Test panic on duplicate key
		require.PanicsWithValue(t, "assertion failed: dvo: property 'name' already exists", func() {
			vo.Add("name", "another-gopher")
		})
		require.PanicsWithValue(t, "assertion failed: dov: property 'a.d' contains '.'", func() {
			vo.Add("a.d", "another-gopher")
		})
	})

	t.Run("Update", func(t *testing.T) {
		vo := valueObject{
			Data: map[string]any{},
		}
		vo.Add("name", "gopher")
		// Test successful Update
		require.NotPanics(t, func() {
			vo.Update("name", "gopher-updated")
		})
		name, ok := vo.Get("name").Get()
		require.True(t, ok)
		require.Equal(t, "gopher-updated", name)

		// Test panic on non-existent key
		require.PanicsWithValue(t, "dvo: property 'age' does not exist", func() {
			vo.Update("age", 30)
		})
	})
}

func TestValueObject_MustMethods(t *testing.T) {
	now := time.Now()
	vo := valueObject{
		Data: map[string]any{
			"my_string":  "hello",
			"my_int":     int(-1),
			"my_int8":    int8(-8),
			"my_int16":   int16(-16),
			"my_int32":   int32(-32),
			"my_int64":   int64(-64),
			"my_uint":    uint(1),
			"my_uint8":   uint8(8),
			"my_uint16":  uint16(16),
			"my_uint32":  uint32(32),
			"my_uint64":  uint64(64),
			"my_float32": float32(32.32),
			"my_float64": float64(64.64),
			"my_bool":    true,
			"my_time":    now,
		}}

	t.Run("successful gets", func(t *testing.T) {
		require.Equal(t, "hello", vo.MstString("my_string"))
		require.Equal(t, int(-1), vo.MstInt("my_int"))
		require.Equal(t, int8(-8), vo.MstInt8("my_int8"))
		require.Equal(t, int16(-16), vo.MstInt16("my_int16"))
		require.Equal(t, int32(-32), vo.MstInt32("my_int32"))
		require.Equal(t, int64(-64), vo.MstInt64("my_int64"))
		require.Equal(t, uint(1), vo.MstUint("my_uint"))
		require.Equal(t, uint8(8), vo.MstUint8("my_uint8"))
		require.Equal(t, uint16(16), vo.MstUint16("my_uint16"))
		require.Equal(t, uint32(32), vo.MstUint32("my_uint32"))
		require.Equal(t, uint64(64), vo.MstUint64("my_uint64"))
		require.Equal(t, float32(32.32), vo.MstFloat32("my_float32"))
		require.Equal(t, float64(64.64), vo.MstFloat64("my_float64"))
		require.Equal(t, true, vo.MstBool("my_bool"))
		require.Equal(t, now, vo.MstTime("my_time"))
	})

	t.Run("panic on missing key", func(t *testing.T) {
		require.Panics(t, func() { vo.MstString("nonexistent") })
		require.Panics(t, func() { vo.MstInt("nonexistent") })
		require.Panics(t, func() { vo.MstInt8("nonexistent") })
		require.Panics(t, func() { vo.MstInt16("nonexistent") })
		require.Panics(t, func() { vo.MstInt32("nonexistent") })
		require.Panics(t, func() { vo.MstInt64("nonexistent") })
		require.Panics(t, func() { vo.MstUint("nonexistent") })
		require.Panics(t, func() { vo.MstUint8("nonexistent") })
		require.Panics(t, func() { vo.MstUint16("nonexistent") })
		require.Panics(t, func() { vo.MstUint32("nonexistent") })
		require.Panics(t, func() { vo.MstUint64("nonexistent") })
		require.Panics(t, func() { vo.MstFloat32("nonexistent") })
		require.Panics(t, func() { vo.MstFloat64("nonexistent") })
		require.Panics(t, func() { vo.MstBool("nonexistent") })
		require.Panics(t, func() { vo.MstTime("nonexistent") })
	})
}

func TestField_PanicOnInvalidName(t *testing.T) {
	t.Run("invalid name with dot", func(t *testing.T) {
		require.PanicsWithValue(t, "dvo: field name 'user.name' cannot contain '.' or '#'", func() {
			Field[string]("user.name")
		})
	})

	t.Run("invalid name with hash", func(t *testing.T) {
		require.PanicsWithValue(t, "dvo: field name 'user#name' cannot contain '.' or '#'", func() {
			Field[string]("user#name")
		})
	})
}

func TestField_PanicOnDuplicateValidator(t *testing.T) {
	t.Run("duplicate validator", func(t *testing.T) {
		require.PanicsWithValue(t, "dvo: duplicate validator 'min_length' for field 'password'", func() {
			Field[string]("password", validator.MinLength(5), validator.MinLength(10))
		})
	})
}

func TestNestedValidation(t *testing.T) {
	userSchema := WithFields(
		Field[string]("name", validator.MinLength(1)),
		Field[string]("email", validator.Email()),
	)

	supplierSchema := WithFields(
		Field[string]("id"),
		Field[string]("name", validator.MinLength(1)),
	)

	itemSchema := WithFields(
		Field[int]("id", validator.Gt(0)),
		Field[string]("name", validator.MinLength(1)),
		// Add supplier as an optional 3rd level object to not break existing tests
		ObjectField("supplier", supplierSchema).Optional(),
	)

	requestSchema := WithFields(
		Field[string]("id"),
		ObjectField("user", userSchema),
		ArrayField[string]("tags", validator.MinLength(2)),
		ArrayOfObjectField("items", itemSchema),
		ArrayField[string]("string_array").Optional(),
		ArrayField[int]("int_array").Optional(),
		ArrayField[int64]("int64_array").Optional(),
		ArrayField[float64]("float64_array").Optional(),
		ArrayField[bool]("bool_array").Optional(),
	)

	tests := []struct {
		name        string
		jsonFile    string
		isValid     bool
		check       func(t *testing.T, vo ValueObject)
		errContains string
	}{
		{
			name:     "valid embedded json",
			jsonFile: "nested_valid.json",
			isValid:  true,
			check: func(t *testing.T, vo ValueObject) {
				id, _ := vo.String("id").Get()
				require.Equal(t, "req-123", id)

				// check embedded object
				user, _ := vo.Get("user").Get()
				userVO := user.(ValueObject)
				require.Equal(t, "John Doe", userVO.MstString("name"))
				require.Equal(t, "john.doe@example.com", userVO.MstString("email"))

				// check array of primitive
				tags, _ := vo.Get("tags").Get()
				require.Equal(t, []string{"go", "dvo", "testing"}, tags)

				// check array of objects
				items, _ := vo.Get("items").Get()
				itemsVO := items.([]ValueObject)
				require.Len(t, itemsVO, 2)
				require.Equal(t, 1, itemsVO[0].MstInt("id"))
				require.Equal(t, "Item A", itemsVO[0].MstString("name"))
				require.Equal(t, 2, itemsVO[1].MstInt("id"))
				require.Equal(t, "Item B", itemsVO[1].MstString("name"))
			},
		},
		{
			name:        "invalid embedded user object",
			jsonFile:    "nested_invalid_user.json",
			isValid:     false,
			errContains: "email: field 'email': not valid email address",
		},
		{
			name:        "invalid array of primitives",
			jsonFile:    "nested_invalid_tags.json",
			isValid:     false,
			errContains: "tags[1]: type mismatch: expected string but got raw type Number",
		},
		{
			name:        "invalid array of objects",
			jsonFile:    "nested_invalid_item.json",
			isValid:     false,
			errContains: "items[1]: field 'id': must be greater than 0",
		},
		{
			name:        "invalid array type",
			jsonFile:    "nested_invalid_array_type.json",
			isValid:     false,
			errContains: "field 'tags' expected a JSON array but got Clause",
		},
		{
			name:        "invalid item type in array of objects",
			jsonFile:    "nested_invalid_item_type.json",
			isValid:     false,
			errContains: "items[1]: expected a JSON object but got Clause",
		},
		{
			name:        "invalid tag validator",
			jsonFile:    "nested_invalid_tag_validator.json",
			isValid:     false,
			errContains: "tags[1]: length must be at least 2",
		},
		{
			name:     "valid 3-level embedded json",
			jsonFile: "nested_3_level_valid.json",
			isValid:  true,
			check: func(t *testing.T, vo ValueObject) {
				// Test hierarchical access using the Get() method, which calls get[any]
				email, _ := vo.Get("user.email").Get()
				require.Equal(t, "deep.validator@example.com", email)

				email = vo.MstString("user.email")
				require.Equal(t, "deep.validator@example.com", email)

				tag, _ := vo.Get("tags.1").Get()
				require.Equal(t, "dvo", tag)

				itemID, _ := vo.Get("items.0.id").Get()
				require.Equal(t, 101, itemID)

				// Test getting a embedded object and then accessing its properties
				supplier := vo.Get("items.0.supplier").MustGet()
				supplierVO := supplier.(ValueObject)
				require.Equal(t, "SUP-A", supplierVO.MstString("id"))
				require.Equal(t, "Supplier Alpha", supplierVO.MstString("name"))

				// check primitive arrays
				require.Equal(t, []string{"a", "b", "c"}, vo.MstStringArray("string_array"))
				require.Equal(t, "a", vo.MstString("string_array.0"))
				require.Equal(t, "c", vo.MstString("string_array.2"))
				require.Equal(t, []int{1, 2, 3}, vo.MstIntArray("int_array"))
				require.Equal(t, []int64{10, 20, 30}, vo.MstInt64Array("int64_array"))
				require.Equal(t, []float64{1.1, 2.2, 3.3}, vo.MstFloat64Array("float64_array"))
				require.Equal(t, []bool{true, false, true}, vo.MstBoolArray("bool_array"))
			},
		},
		{
			name:     "panic on invalid array index format",
			jsonFile: "nested_valid.json",
			isValid:  true,
			check: func(t *testing.T, vo ValueObject) {
				require.PanicsWithValue(t,
					"assertion failed: dvo: path part 'not-an-index' in 'tags.not-an-index' is not a valid integer index for a slice",
					func() {
						vo.Get("tags.not-an-index")
					})
			},
		},
		{
			name:     "panic on out of bounds array index",
			jsonFile: "nested_valid.json",
			isValid:  true,
			check: func(t *testing.T, vo ValueObject) {
				require.PanicsWithValue(t,
					"assertion failed: dvo: array bound exceed: [go dvo testing]",
					func() {
						vo.Get("tags.3")
					})
			},
		},
		{
			name:     "traverse into primitive returns none",
			jsonFile: "nested_valid.json",
			isValid:  true,
			check: func(t *testing.T, vo ValueObject) {
				// 'user.name' is a string. Traversing further into it should fail gracefully.
				opt := vo.Get("user.name.foo")
				require.False(t, opt.IsPresent(), "expected None when traversing into a primitive")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jsonPath := filepath.Join("testdata", tc.jsonFile)
			jsonData, err := os.ReadFile(jsonPath)
			require.NoError(t, err, "failed to read test data file")

			res := requestSchema.Validate(string(jsonData))

			if tc.isValid {
				require.False(t, res.IsError(), "validation failed unexpectedly: %v", res.Error())
				if tc.check != nil {
					tc.check(t, res.MustGet())
				}
			} else {
				require.True(t, res.IsError(), "validation should have failed")
				if tc.errContains != "" {
					msg := res.Error().Error()
					require.Contains(t, msg, tc.errContains)
				}
			}
		})
	}
}

func TestSchema_Validate(t *testing.T) {
	// Define a base Schema for testing various validation scenarios.
	testSchema := WithFields(
		Field[string]("name"),
		Field[int]("id"),
		ObjectField("user", WithFields(Field[string]("email"))).Optional(),
	)

	tests := []struct {
		name        string
		vo          *Schema
		json        string
		urlParams   []map[string]string
		wantErr     bool
		errContains string
		check       func(t *testing.T, vo ValueObject)
	}{
		{
			name:        "invalid json",
			vo:          testSchema,
			json:        `{"id": 123,}`,
			urlParams:   nil,
			wantErr:     true,
			errContains: "invalid json",
		},
		{
			name:        "duplicated url parameter across maps",
			vo:          testSchema,
			json:        "",
			urlParams:   []map[string]string{{"id": "1"}, {"id": "2"}},
			wantErr:     true,
			errContains: "duplicated url parameter 'id'",
		},
		{
			name:        "unknown url parameter disallowed",
			vo:          testSchema,
			json:        "",
			urlParams:   []map[string]string{{"extra": "param"}},
			wantErr:     true,
			errContains: "unknown url parameter 'extra'",
		},
		{
			name: "unknown url parameter allowed",
			vo:   WithFields(Field[string]("name")).AllowUnknownFields(),
			json: `{"name":"gopher"}`,
			urlParams: []map[string]string{
				{"extra": "param"},
				{"another": "value"},
			},
			wantErr: false,
			check: func(t *testing.T, vo ValueObject) {
				// The unknown params should be in the final object
				extra, ok := vo.String("extra").Get()
				require.True(t, ok)
				require.Equal(t, "param", extra)
				another, ok := vo.String("another").Get()
				require.True(t, ok)
				require.Equal(t, "value", another)
			},
		},
		{
			name:        "url parameter mapped to embedded object",
			vo:          testSchema,
			json:        "",
			urlParams:   []map[string]string{{"user": "some-value"}},
			wantErr:     true,
			errContains: "url parameter 'user' is mapped to a embedded object",
		},
		{
			name:        "conflict between json and url parameter",
			vo:          testSchema,
			json:        `{"id": 123}`,
			urlParams:   []map[string]string{{"id": "456"}},
			wantErr:     true,
			errContains: "duplicate parameter in url and json 'id'",
		},
		{
			name:      "valid with json and url params",
			vo:        testSchema,
			json:      `{"name": "gopher"}`,
			urlParams: []map[string]string{{"id": "123"}},
			wantErr:   false,
			check: func(t *testing.T, vo ValueObject) {
				name, _ := vo.String("name").Get()
				require.Equal(t, "gopher", name)
				id, _ := vo.Int("id").Get()
				require.Equal(t, 123, id)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := tc.vo.Validate(tc.json, tc.urlParams...)

			if tc.wantErr {
				require.True(t, res.IsError(), "expected an error but got none")
				if tc.errContains != "" {
					require.Contains(t, res.Error().Error(), tc.errContains, "error message does not contain expected text")
				}
			} else {
				require.False(t, res.IsError(), "got unexpected error: %v", res.Error())
				if tc.check != nil {
					tc.check(t, res.MustGet())
				}
			}
		})
	}
}

func TestSchema_Extend(t *testing.T) {
	baseSchema := WithFields(
		Field[string]("id"),
		Field[time.Time]("createdAt"),
	)

	userSpecificSchema := WithFields(
		Field[string]("name"),
		Field[string]("email"),
	)

	t.Run("successful extend", func(t *testing.T) {
		userSchema := baseSchema.Extend(userSpecificSchema)
		require.Len(t, userSchema.fields, 4)

		// Verify all fields are present
		fieldNames := make(map[string]bool)
		for _, f := range userSchema.fields {
			fieldNames[f.Name()] = true
		}
		require.True(t, fieldNames["id"])
		require.True(t, fieldNames["createdAt"])
		require.True(t, fieldNames["name"])
		require.True(t, fieldNames["email"])
	})

	t.Run("panic on duplicate field", func(t *testing.T) {
		conflictingSchema := WithFields(
			Field[string]("id"), // Duplicate field
		)
		require.PanicsWithValue(t, "dvo: duplicate field name 'id' found during Extend", func() {
			baseSchema.Extend(conflictingSchema)
		})
	})

	t.Run("extend with empty schema", func(t *testing.T) {
		emptySchema := WithFields()
		extended := baseSchema.Extend(emptySchema)
		require.Len(t, extended.fields, 2)
		require.Equal(t, baseSchema.fields, extended.fields)

		extendedFromEmpty := emptySchema.Extend(baseSchema)
		require.Len(t, extendedFromEmpty.fields, 2)
		require.Equal(t, baseSchema.fields, extendedFromEmpty.fields)
	})

	t.Run("allow unknown fields propagation", func(t *testing.T) {
		schemaA := WithFields(Field[string]("a"))
		schemaB := WithFields(Field[string]("b")).AllowUnknownFields()
		schemaC := WithFields(Field[string]("c"))

		ab := schemaA.Extend(schemaB)
		require.True(t, ab.allowUnknownFields)

		ac := schemaA.Extend(schemaC)
		require.False(t, ac.allowUnknownFields)
	})
}

// Tests for WrapField adapter (migrated from persistent_adapter_test.go)

type dummyEntity struct{}

func (dummyEntity) Table() string { return "dummy" }

func TestWrapField_Basics(t *testing.T) {
	f := xql.NewField[dummyEntity, string]("unit_price", "UnitPrice")
	vf := WrapField[string](f)
	require.NotNil(t, vf)
	// QualifiedName should delegate to the persistent field
	require.Equal(t, f.QualifiedName(), vf.QualifiedName())
	// Name should be the last segment (view) -> "UnitPrice"
	require.Equal(t, "UnitPrice", vf.Name())
	// Scope should delegate
	require.Equal(t, f.Scope(), vf.Scope())
}

func TestWrapField_Validators(t *testing.T) {
	f := xql.NewField[dummyEntity, string]("username", "Username")
	// Use a built-in validator factory
	vf := WrapField[string](f, validator.MinLength(3))
	// validateRaw should enforce min length
	r := vf.validateRaw("ab")
	require.True(t, r.IsError())
	r = vf.validateRaw("abcd")
	require.False(t, r.IsError())
}
