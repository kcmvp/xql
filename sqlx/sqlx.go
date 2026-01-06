package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/kcmvp/xql"
	"github.com/kcmvp/xql/entity"
	"github.com/kcmvp/xql/internal"
	"github.com/samber/lo"
	"github.com/samber/mo"
)

// Package sqlx
//
// sqlx provides a small, generator-friendly SQL DSL and execution layer
// for single-table CRUD operations. The package is intentionally compact
// and designed to work with generator-produced metadata (`meta.Schema`,
// `meta.Field`, `meta.ValueObject`).
//
// Design principles (short):
//  - SQL generation is pure: `sql()` returns only the SQL string and error.
//    Execution-time arguments are produced by the lower-level builder
//    helpers (e.g. `selectSQL`, `insertSQL`, `updateSQL`, `deleteSQL`).
//  - Public API is tiny: consumers construct `Executor` via `Query` /
//    `Delete` / `Update` factory functions and call `Execute(ctx, db)` to
//    run the statement. `Execute` returns a union `mo.Either` value where
//    the left side is `[]meta.ValueObject` (SELECT results) and the right
//    side is `sql.Result` (non-query statements).
//  - The package depends on generator-provided `meta.Schema` to determine
//    projection order and mapping between SQL result columns and value
//    object keys.

// Where is the only contract used to express predicates for CRUD.
//
// The DSL helpers in this package (And/Or/Eq/In/Like/...) produce values
// that implement `Where`. `Where.Build()` returns a SQL fragment and the
// parameter list suitable for use in a prepared statement.
type Where interface {
	Build() (string, []any)
	// fields returns the referenced xql.Fields used by this Where. It's an
	// unexported method so callers outside this package cannot implement Where
	// (we want internal control over implementations).
	fields() []xql.Field
}

type Schema []xql.Field

// --- Where DSL helpers (public) ---

// And combines multiple Where conditions with the AND operator.
// Nil or empty clauses are ignored.
func And(wheres ...Where) Where {
	return and(wheres...)
}

// Or combines multiple Where conditions with the OR operator.
// Nil or empty clauses are ignored.
func Or(wheres ...Where) Where {
	return or(wheres...)
}

// Eq builds a "field = ?" predicate.
func Eq(field xql.Field, value any) Where {
	return op(field, "=", value)
}

// Ne builds a "field != ?" predicate.
func Ne(field xql.Field, value any) Where {
	return op(field, "!=", value)
}

// Gt builds a "field > ?" predicate.
func Gt(field xql.Field, value any) Where {
	return op(field, ">", value)
}

// Gte builds a "field >= ?" predicate.
func Gte(field xql.Field, value any) Where {
	return op(field, ">=", value)
}

// Lt builds a "field < ?" predicate.
func Lt(field xql.Field, value any) Where {
	return op(field, "<", value)
}

// Lte builds a "field <= ?" predicate.
func Lte(field xql.Field, value any) Where {
	return op(field, "<=", value)
}

// Like builds a "field LIKE ?" predicate.
func Like(field xql.Field, value string) Where {
	return op(field, "LIKE", value)
}

// In builds a "field IN (?, ?, ...)" predicate.
// Empty values produce an always-false clause (1=0).
func In(field xql.Field, values ...any) Where {
	return inWhere(field, values...)
}

// Executor represents the delayed execution step constructed by the
// top-level factory helpers (`Query`, `Delete`, `Update`).
//
// Execution contract:
//   - Callers obtain an Executor via one of the factory functions and then
//     call `Execute(ctx, db)` to run the statement. Parameter values for the
//     prepared statement are set during `Where.Build()` (the Where returns
//     the SQL fragment and its argument list). For INSERT/UPDATE builders
//     which consume a `meta.ValueObject` for column values, those helpers
//     read the ValueObject inside their builder functions.
//   - `Execute` will call the appropriate lower-level builder helper to
//     obtain both the SQL string and the argument list (e.g. `selectSQL`).
//   - For SELECT statements the left side of `mo.Either` holds the
//     `[]meta.ValueObject` results; for non-SELECT statements the right
//     side holds the `sql.Result`.
//
// Note: `sql()` is kept pure and only returns the generated SQL string and
// an error. It is primarily useful for testing and inspection.
type Executor interface {
	Execute(ctx context.Context, ds *sql.DB) (mo.Either[[]ValueObject, sql.Result], error)
	// sql generates the SQL string only (pure). Arguments are produced by lower-level helpers
	// (selectSQL/insertSQL/updateSQL/deleteSQL) and consumed by Execute when running against DB.
	sql() (string, error)
}

// Query builds a single-table SELECT query.
//
// Usage example:
//
//	// build executor
//	// exec := Query[Account](schema)(Eq(field, value))
//	// run
//	// resEither, err := exec.Execute(ctx, db)
//	// check left/right and handle accordingly
func Query[T entity.Entity](schema Schema) func(where Where) Executor {
	return func(where Where) Executor {
		// basic sanity checks
		if schema == nil || len(schema) == 0 {
			return errorExecutorSelect{err: fmt.Errorf("schema is required and must contain at least one field")}
		}
		// collect where fields (may be nil)
		var wfields []xql.Field
		if where != nil {
			wfields = where.fields()
		}
		// single combined validation: ensure all referenced fields (schema + where)
		// belong to the entity table T. validateSyntax handles empty input len==0.
		if err := validateSyntax[T](append([]xql.Field(schema), wfields...)...); err != nil {
			return errorExecutorSelect{err: err}
		}
		return queryExec[T]{schema: schema, where: where}
	}
}

// Delete builds a single-table DELETE query.
//
// Note: per design, callers should provide a non-empty where clause; the
// implementation enforces this at builder time (deleteSQL returns error if
// where is empty). We now validate referenced fields in Where early so callers
// get immediate, clear errors when using fields from the wrong entity.
func Delete[T entity.Entity](where Where) Executor {
	// early validate where fields (if any)
	if where != nil {
		if err := validateSyntax[T](where.fields()...); err != nil {
			return errorExecutorNonSelect{err: err}
		}
	}
	return deleteExec[T]{where: where}
}

// Update builds a single-table UPDATE query.
//
// New design: public Update requires the caller to provide the persistence
// schema explicitly. The Update helper will generate SQL using the provided
// Schema and an optional ValueObject of values to apply.
func Update[T entity.Entity](schema Schema, values ValueObject) func(where Where) Executor {
	return func(where Where) Executor {
		// schema must be provided now
		if schema == nil || len(schema) == 0 {
			return errorExecutorNonSelect{err: fmt.Errorf("schema is required and must contain at least one field")}
		}
		// validate schema fields belong to T
		if err := validateSyntax[T](schema...); err != nil {
			return errorExecutorNonSelect{err: err}
		}

		// validate where fields against T
		if where != nil {
			if err := validateSyntax[T](where.fields()...); err != nil {
				return errorExecutorNonSelect{err: err}
			}
		}
		return updateExec[T]{schema: schema, values: values, where: where}
	}
}

// QueryJoin builds a select executor that injects `joinstmt` into the FROM
// clause. The returned Executor follows the existing `Executor` contract.
func QueryJoin(schema Schema) func(joinstmt string, where Where) Executor {
	return func(joinstmt string, where Where) Executor {
		return joinQueryExec{schema: schema, joinstmt: joinstmt, where: where}
	}
}

// DeleteJoin builds a delete executor that uses an EXISTS-correlated subquery
// to apply the join-based filter. It derives base table from generic type T.
func DeleteJoin[T entity.Entity](joinstmt string, where Where) Executor {
	var ent T
	baseTable := ent.Table()
	return joinDeleteExec{baseTable: baseTable, joinstmt: joinstmt, where: where}
}

// UpdateJoin builds an update executor that applies an EXISTS-correlated
// join filter. The update payload values are supplied as a meta.ValueObject
// when creating the executor via UpdateJoin[T](schema, values)(joinstmt, where).
func UpdateJoin[T entity.Entity](schema Schema, values ValueObject) func(joinstmt string, where Where) Executor {
	return func(joinstmt string, where Where) Executor {
		return updateJoinExec[T]{schema: schema, values: values, joinstmt: joinstmt, where: where}
	}
}

type sealer struct{}

// ValueObject is a thin alias over internal.ValueObject to expose it
type ValueObject interface {
	internal.ValueObject
	seal(sealer)
}

type valueObject struct {
	internal.Data
}

var _ ValueObject = (*valueObject)(nil)

func (vo valueObject) seal(sealer) {}

type Pair struct {
	tuple lo.Tuple2[xql.Field, any]
}

func Tuple[T xql.FieldType](f xql.PersistentField[T], v T) Pair {
	return Pair{tuple: lo.Tuple2[xql.Field, any]{A: &f, B: v}}
}

// FlatMap is the flattened representation expected by SQL helpers. Keys are
// qualified dotted names like "table.column" or "table.column.view".
type FlatMap map[string]any

func MapValueObject(m FlatMap) ValueObject {
	lo.Assert(m != nil && len(m) > 0, "mapValueObject: input map cannot be nil or empty")
	for key := range m {
		parts := strings.Split(key, ".")
		lo.Assert(len(parts) >= 2, "mapValueObject: keys must contain at least table and column (%s)", key)
	}
	return valueObject{Data: internal.Data(m)}
}

func TupleValueObject(pairs ...Pair) ValueObject {
	m := internal.Data{}
	for _, p := range pairs {
		if f := p.tuple.A; f != nil {
			m[f.QualifiedName()] = p.tuple.B
		}
	}
	return valueObject{Data: m}
}

type updateExec[T entity.Entity] struct {
	schema Schema
	values ValueObject
	where  Where
}

func (u updateExec[T]) Execute(ctx context.Context, ds *sql.DB) (mo.Either[[]ValueObject, sql.Result], error) {
	if ds == nil {
		return mo.Right[[]ValueObject, sql.Result](nil), fmt.Errorf("db is required")
	}
	q, args, err := updateSQL[T](u.schema, u.values, u.where)
	if err != nil {
		return mo.Right[[]ValueObject, sql.Result](nil), err
	}
	res, err := ds.ExecContext(ctx, q, args...)
	if err != nil {
		return mo.Right[[]ValueObject, sql.Result](nil), err
	}
	return mo.Right[[]ValueObject, sql.Result](res), nil
}

func (u updateExec[T]) sql() (string, error) {
	q, _, err := updateSQL[T](u.schema, u.values, u.where)
	return q, err
}

// updateJoinExec implements update with join-based EXISTS filter.
type updateJoinExec[T entity.Entity] struct {
	schema   Schema
	values   ValueObject
	joinstmt string
	where    Where
}

func (u updateJoinExec[T]) Execute(ctx context.Context, ds *sql.DB) (mo.Either[[]ValueObject, sql.Result], error) {
	if ds == nil {
		return mo.Right[[]ValueObject, sql.Result](nil), fmt.Errorf("db is required")
	}
	// build a Where representing the EXISTS(...) predicate (applies joinstmt and inner where)
	existsWhere, err := buildExistsWhere(u.joinstmt, u.where)
	if err != nil {
		return mo.Right[[]ValueObject, sql.Result](nil), err
	}
	q, args, err := updateSQL[T](u.schema, u.values, existsWhere)
	if err != nil {
		return mo.Right[[]ValueObject, sql.Result](nil), err
	}
	res, err := ds.ExecContext(ctx, q, args...)
	if err != nil {
		return mo.Right[[]ValueObject, sql.Result](nil), err
	}
	return mo.Right[[]ValueObject, sql.Result](res), nil
}

func (u updateJoinExec[T]) sql() (string, error) {
	existsWhere, err := buildExistsWhere(u.joinstmt, u.where)
	if err != nil {
		return "", err
	}
	ustr, _, err := updateSQL[T](u.schema, u.values, existsWhere)
	return ustr, err
}
