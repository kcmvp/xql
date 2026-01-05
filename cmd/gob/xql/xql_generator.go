package xql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/constant"
	"go/format"
	"go/token"
	"go/types"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	_ "embed"

	"github.com/kcmvp/xql/cmd/internal"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/tools/go/packages"
)

//go:embed resources/fields.tmpl
var fieldsTmpl string

//go:embed resources/schema.tmpl
var schemaTmpl string

// SchemaTemplateData holds the data passed to the schema template.
type SchemaTemplateData struct {
	TableName   string
	Fields      []Field
	GeneratedAt time.Time
	Version     string
}

// TemplateData holds the data passed to the template for execution.
type TemplateData struct {
	PackageName      string
	StructName       string
	Imports          []string
	Fields           []Field
	ModulePath       string
	ModulePkgName    string
	EntityImportPath string
	GeneratedAt      time.Time
	Version          string
}

// Field represents a single column in a database table, derived from a Go struct field.
type Field struct {
	Name          string // The database column name (e.g., "creation_time").
	GoName        string // The original Go field name (e.g., "CreatedAt").
	GoType        string // The Go type of the field (e.g., "time.Time").
	DBType        string // The specific SQL type for the column (e.g., "TIMESTAMP WITH TIME ZONE").
	IsPK          bool   // True if this field is the primary key.
	IsNotNull     bool   // True if the column has a NOT NULL constraint.
	IsUnique      bool   // True if the column has a UNIQUE constraint.
	IsIndexed     bool   // True if an index should be created on this column.
	Default       string // The default value for the column, as a string.
	FKTable       string // The table referenced by a foreign key.
	FKColumn      string // The column referenced by a foreign key.
	Warning       string // A warning message associated with this field, e.g., for discouraged PK types.
	IsEmbedded    bool
	ValidatorArgs string // pre-rendered validator arguments (prefixed with ", ") to inject into templates
}

// isSupportedType checks if a field type is valid.
func isSupportedType(typ types.Type) bool {
	// Check for named types like time.Time
	if named, ok := typ.(*types.Named); ok {
		if named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "time" && named.Obj().Name() == "Time" {
			return true
		}
	}

	// Check for basic types
	basic, ok := typ.Underlying().(*types.Basic)
	if !ok {
		return false
	}

	// Explicit kind set instead of a switch to avoid IDE warnings about missing iota cases.
	allowed := map[types.BasicKind]struct{}{
		types.Bool:    {},
		types.Int:     {},
		types.Int8:    {},
		types.Int16:   {},
		types.Int32:   {},
		types.Int64:   {},
		types.Uint:    {},
		types.Uint8:   {},
		types.Uint16:  {},
		types.Uint32:  {},
		types.Uint64:  {},
		types.Float32: {},
		types.Float64: {},
		types.String:  {},
	}
	_, ok = allowed[basic.Kind()]
	return ok
}

// applyOrderPolicy reorders a slice of fields based on the defined ordering policy:
// 1. Primary key fields
// 2. Host struct fields
// 3. Embedded struct fields
func applyOrderPolicy(fields []Field) []Field {
	var pkFields []Field
	var hostFields []Field
	var embeddedFields []Field

	for _, f := range fields {
		if f.IsPK {
			pkFields = append(pkFields, f)
		} else if f.IsEmbedded {
			embeddedFields = append(embeddedFields, f)
		} else {
			hostFields = append(hostFields, f)
		}
	}

	return append(append(pkFields, hostFields...), embeddedFields...)
}

// EntityMeta holds all the derived metadata needed to generate both field helpers
// and database schemas for one entity.
//
// Fields are ordered using applyOrderPolicy.
type EntityMeta struct {
	StructName string
	PkgPath    string
	Pkg        *packages.Package
	TypeSpec   *ast.TypeSpec
	TableName  string
	Fields     []Field // adapter-agnostic field info (no DBType)
}

// OutputWriter abstracts file writing so generation can be directed to disk or memory (tests).
type OutputWriter interface {
	MkdirAll(path string, perm os.FileMode) error
	WriteFile(path string, data []byte, perm os.FileMode) error
}

// DiskWriter writes files to the real filesystem.
type DiskWriter struct{}

func (DiskWriter) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (DiskWriter) WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}

// MemoryWriter captures written files in-memory (map[path]content).
// Useful for tests to avoid mutating the repository.
type MemoryWriter struct {
	Files map[string][]byte
}

func NewMemoryWriter() *MemoryWriter {
	return &MemoryWriter{Files: map[string][]byte{}}
}

func (m *MemoryWriter) MkdirAll(path string, perm os.FileMode) error {
	// no-op for memory
	return nil
}

func (m *MemoryWriter) WriteFile(path string, data []byte, perm os.FileMode) error {
	m.Files[path] = append([]byte(nil), data...)
	return nil
}

// generateWithWriter runs generation and writes outputs using the provided OutputWriter.
// It returns the in-memory map when a *MemoryWriter is used, otherwise nil.
func generateWithWriter(ctx context.Context, w OutputWriter) (map[string][]byte, error) {
	project := internal.Current
	if project == nil {
		return nil, fmt.Errorf("project context not initialized")
	}

	adapters, ok := ctx.Value(dbaAdapterKey).([]string)
	if !ok || len(adapters) == 0 {
		return nil, fmt.Errorf("no database adapters are configured or detected")
	}

	// entity filter may be provided in context
	// NOTE: generation-side filtering is handled inside generateMeta by reading
	// ctx.Value(entityFilterKey). The local entityFilter variable here was
	// redundant and caused a compile error by being unused; removed.

	// metas, err := buildEntityMeta(ctx, entityFilter)
	metas, err := generateMeta(ctx)
	if err != nil {
		return nil, err
	}

	// prepare templates
	fieldTmpl, err := template.New("fields").Funcs(template.FuncMap{
		"ago": func(t time.Time) string { return t.Format(time.RFC3339) },
	}).Parse(fieldsTmpl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse fields template: %w", err)
	}
	funcMap := template.FuncMap{
		"plus1": func(i int) int { return i + 1 },
	}
	schemaTmplParsed, err := template.New("schema").Funcs(funcMap).Parse(schemaTmpl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema template: %w", err)
	}

	// precompile regexes
	varcharRe := regexp.MustCompile(`(?i)^varchar\((\d+)\)`)                                  // capture length
	decimalRe := regexp.MustCompile(`(?i)^(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)`) // capture precision,scale

	for _, meta := range metas {
		imports := buildImports(meta)

		// compute module package name heuristically: try to load package to get declared name,
		// fall back to last path element if load fails.
		modulePkgName := path.Base(internal.ToolModulePath())
		if pkgs, _ := packages.Load(&packages.Config{Mode: packages.NeedName}, internal.ToolModulePath()); len(pkgs) > 0 {
			if pkgs[0].Name != "" {
				modulePkgName = pkgs[0].Name
			}
		}

		// make a copy of fields so we can annotate ValidatorArgs per-field
		fieldsCopy := make([]Field, len(meta.Fields))
		copy(fieldsCopy, meta.Fields)

		// build validator args based on DBType
		for i := range fieldsCopy {
			f := &fieldsCopy[i]
			db := strings.TrimSpace(strings.ToLower(f.DBType))
			var args []string
			if db != "" {
				if f.GoType == "string" {
					if m := varcharRe.FindStringSubmatch(db); len(m) == 2 {
						n := m[1]
						args = append(args, fmt.Sprintf("%s.MaxLength(%s)", modulePkgName, n))
					} else if m := decimalRe.FindStringSubmatch(db); len(m) == 3 {
						p := m[1]
						s := m[2]
						args = append(args, fmt.Sprintf("%s.Decimal(%s, %s)", modulePkgName, p, s))
					}
				} else {
					if m := decimalRe.FindStringSubmatch(db); len(m) == 3 {
						p, _ := strconv.Atoi(m[1])
						s, _ := strconv.Atoi(m[2])
						switch f.GoType {
						case "float32", "float64":
							args = append(args, fmt.Sprintf("%s.Decimal[%s](%s, %s)", modulePkgName, f.GoType, m[1], m[2]))
						default:
							intDigits := p - s
							if intDigits < 1 {
								intDigits = 1
							}
							maxInt := int64(1)
							for k := 0; k < intDigits; k++ {
								maxInt *= 10
							}
							maxInt = maxInt - 1
							switch f.GoType {
							case "int", "int8", "int16", "int32", "int64":
								args = append(args, fmt.Sprintf("%s.Gte[%s](%d)", modulePkgName, f.GoType, -maxInt))
								args = append(args, fmt.Sprintf("%s.Lte[%s](%d)", modulePkgName, f.GoType, maxInt))
							case "uint", "uint8", "uint16", "uint32", "uint64":
								args = append(args, fmt.Sprintf("%s.Gte[%s](%d)", modulePkgName, f.GoType, 0))
								args = append(args, fmt.Sprintf("%s.Lte[%s](%d)", modulePkgName, f.GoType, maxInt))
							}
						}
					}
				}
			}
			if len(args) > 0 {
				f.ValidatorArgs = ", " + strings.Join(args, ", ")
			} else {
				f.ValidatorArgs = ""
			}
		}

		data := TemplateData{
			PackageName:      strings.ToLower(meta.StructName),
			StructName:       meta.StructName,
			Imports:          imports,
			Fields:           fieldsCopy,
			ModulePath:       internal.ToolModulePath(),
			ModulePkgName:    modulePkgName,
			EntityImportPath: meta.PkgPath,
			GeneratedAt:      time.Now(),
			Version:          computeEntityVersion(meta),
		}

		// render fields template
		var buf bytes.Buffer
		if err := fieldTmpl.Execute(&buf, data); err != nil {
			return nil, fmt.Errorf("failed to execute template for %s: %w", meta.StructName, err)
		}
		formatted, err := format.Source(buf.Bytes())
		if err != nil {
			return nil, fmt.Errorf("failed to format generated code for %s: %w", meta.StructName, err)
		}
		outputDir := filepath.Join(project.GenPath(), "field", data.PackageName)
		if err := w.MkdirAll(outputDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory %s: %w", outputDir, err)
		}
		outputPath := filepath.Join(outputDir, fmt.Sprintf("%s_gen.go", data.PackageName))
		if err := w.WriteFile(outputPath, formatted, 0644); err != nil {
			return nil, fmt.Errorf("failed to write generated file for %s: %w", meta.StructName, err)
		}

		// render schemas for adapters
		for _, adapter := range adapters {
			fields := enrichFieldsForAdapter(meta.Fields, adapter)
			if len(fields) == 0 {
				continue
			}
			data := SchemaTemplateData{
				TableName:   meta.TableName,
				Fields:      fields,
				GeneratedAt: time.Now(),
				Version:     computeEntityVersion(meta),
			}
			var sb bytes.Buffer
			if err := schemaTmplParsed.Execute(&sb, data); err != nil {
				return nil, fmt.Errorf("failed to execute schema template for %s: %w", meta.StructName, err)
			}
			outputDir := filepath.Join(project.GenPath(), "schemas", adapter)
			if err := w.MkdirAll(outputDir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create output directory %s: %w", outputDir, err)
			}
			outputPath := filepath.Join(outputDir, fmt.Sprintf("%s_schema.sql", lo.SnakeCase(meta.StructName)))
			if err := w.WriteFile(outputPath, sb.Bytes(), 0644); err != nil {
				return nil, fmt.Errorf("failed to write generated schema for %s: %w", meta.StructName, err)
			}
		}
	}

	// If writer is a MemoryWriter, return its files for test inspection
	if mw, ok := w.(*MemoryWriter); ok {
		return mw.Files, nil
	}
	return nil, nil
}

// generateToMemory runs the generation and returns generated files in-memory.
func generateToMemory(ctx context.Context) (map[string][]byte, error) {
	mw := NewMemoryWriter()
	return generateWithWriter(ctx, mw)
}

// generate is the single entrypoint for this package's generation workflow.
// It builds entity metadata once, then generates both field helpers and schemas.
func generate(ctx context.Context) error {
	_, err := generateWithWriter(ctx, DiskWriter{})
	return err
}

// generateMeta builds a consistent metadata model from source code exactly once.
// Both field helpers and schema generation should consume this output to avoid
// drift and duplicated parsing logic.
func generateMeta(ctx context.Context) ([]EntityMeta, error) {
	project := internal.Current
	if project == nil {
		return nil, fmt.Errorf("project context not initialized")
	}

	entities := project.StructsImplementEntity()
	// Optional entity filtering:
	// - []string: explicit allow-list of struct names
	// - func(internal.EntityInfo) bool: advanced/internal filtering
	if v := ctx.Value(entityFilterKey); v != nil {
		switch vv := v.(type) {
		case []string:
			allow := make(map[string]struct{}, len(vv))
			for _, n := range vv {
				n = strings.TrimSpace(n)
				if n != "" {
					allow[n] = struct{}{}
				}
			}
			if len(allow) > 0 {
				entities = lo.Filter(entities, func(e internal.EntityInfo, _ int) bool {
					if e.TypeSpec == nil || e.TypeSpec.Name == nil {
						return false
					}
					_, ok := allow[e.TypeSpec.Name.Name]
					return ok
				})
			}
		case func(internal.EntityInfo) bool:
			entities = lo.Filter(entities, func(e internal.EntityInfo, _ int) bool {
				return vv(e)
			})
		}
	}

	if len(entities) == 0 {
		return nil, fmt.Errorf("no entity structs found")
	}

	metas := make([]EntityMeta, 0, len(entities))
	for _, entityInfo := range entities {
		structName := entityInfo.TypeSpec.Name.Name

		fields, err := parseFields(entityInfo.Pkg, entityInfo.TypeSpec, "")
		if err != nil {
			return nil, err
		}
		if len(fields) == 0 {
			return nil, fmt.Errorf("no supported fields found for entity %s", structName)
		}
		fields = applyOrderPolicy(fields)

		tableName, err := resolveTableName(project, entityInfo.PkgPath, structName)
		if err != nil {
			return nil, err
		}

		metas = append(metas, EntityMeta{
			StructName: structName,
			PkgPath:    entityInfo.PkgPath,
			Pkg:        entityInfo.Pkg,
			TypeSpec:   entityInfo.TypeSpec,
			TableName:  tableName,
			Fields:     fields,
		})
	}

	if len(metas) == 0 {
		return nil, fmt.Errorf("no entity structs found")
	}
	return metas, nil
}

func resolveTableName(project *internal.Project, pkgPath, structName string) (string, error) {
	// default fallback
	tableName := lo.SnakeCase(structName)

	// Find Table() method receiver matching structName in that package.
	for _, pkg := range project.Pkgs {
		if pkg.PkgPath != pkgPath {
			continue
		}
		for _, file := range pkg.Syntax {
			ast.Inspect(file, func(n ast.Node) bool {
				fn, ok := n.(*ast.FuncDecl)
				if !ok || fn.Name == nil || fn.Name.Name != "Table" {
					return true
				}
				if fn.Recv == nil || len(fn.Recv.List) == 0 {
					return true
				}

				recvMatches := func(t ast.Expr) bool {
					switch rt := t.(type) {
					case *ast.StarExpr:
						if ident, ok := rt.X.(*ast.Ident); ok {
							return ident.Name == structName
						}
					case *ast.Ident:
						return rt.Name == structName
					}
					return false
				}

				if !recvMatches(fn.Recv.List[0].Type) {
					return true
				}

				// Try to find a return statement and evaluate its result to a stable string.
				if fn.Body == nil || len(fn.Body.List) == 0 {
					return true
				}

				// look for the first ReturnStmt with at least one result
				var retExpr ast.Expr
				for _, stmt := range fn.Body.List {
					if r, ok := stmt.(*ast.ReturnStmt); ok && len(r.Results) > 0 {
						retExpr = r.Results[0]
						break
					}
				}
				if retExpr == nil {
					return true
				}

				if s, ok := evalStringExpr(retExpr, pkg, file); ok {
					tableName = s
					return false
				}
				return true
			})
		}
	}

	return tableName, nil
}

// evalStringExpr attempts to evaluate an AST expression to a string constant.
// It supports:
//   - string basic literals
//   - identifiers that reference package-level constants in the same package
//   - selector expressions referencing imported package constants (best-effort)
//   - parenthesized expressions
//   - binary concatenation using + (recursive)
func evalStringExpr(expr ast.Expr, pkg *packages.Package, file *ast.File) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			return strings.Trim(e.Value, `"`), true
		}
		return "", false
	case *ast.Ident:
		// try to resolve an identifier to a constant via types info
		if obj := pkg.TypesInfo.Uses[e]; obj != nil {
			if c, ok := obj.(*types.Const); ok {
				if val := c.Val(); val != nil {
					if val.Kind() == constant.String {
						return constant.StringVal(val), true
					}
				}
			}
		}
		// also check defs (in-case of const defined in the same file)
		if obj := pkg.TypesInfo.Defs[e]; obj != nil {
			if c, ok := obj.(*types.Const); ok {
				if val := c.Val(); val != nil {
					if val.Kind() == constant.String {
						return constant.StringVal(val), true
					}
				}
			}
		}
		return "", false
	case *ast.SelectorExpr:
		// try to resolve imported package constant: X.Sel
		if ident, ok := e.X.(*ast.Ident); ok {
			pkgName := ident.Name
			sel := e.Sel.Name
			// find corresponding import path in the file's imports
			for _, imp := range file.Imports {
				impPath := strings.Trim(imp.Path.Value, `"`)
				local := ""
				if imp.Name != nil {
					local = imp.Name.Name
				} else {
					local = path.Base(impPath)
				}
				if local == pkgName {
					// find loaded package with this path
					for _, p := range pkg.Types.Imports() {
						if p.Path() == impPath {
							if obj := p.Scope().Lookup(sel); obj != nil {
								if c, ok := obj.(*types.Const); ok {
									if val := c.Val(); val != nil {
										if val.Kind() == constant.String {
											return constant.StringVal(val), true
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return "", false
	case *ast.BinaryExpr:
		if e.Op == token.ADD {
			l, lok := evalStringExpr(e.X, pkg, file)
			r, rok := evalStringExpr(e.Y, pkg, file)
			if lok && rok {
				return l + r, true
			}
		}
		return "", false
	case *ast.ParenExpr:
		return evalStringExpr(e.X, pkg, file)
	default:
		return "", false
	}
}

// generateFieldsFromMeta generates field helpers from the precomputed entity metadata.
func generateFieldsFromMeta(metas []EntityMeta) error {
	project := internal.Current
	if project == nil {
		return fmt.Errorf("project context not initialized")
	}

	tmpl, err := template.New("fields").Parse(fieldsTmpl)
	if err != nil {
		return fmt.Errorf("failed to parse fields template: %w", err)
	}

	// precompile regexes
	varcharRe := regexp.MustCompile(`(?i)^varchar\((\d+)\)`)                                  // capture length
	decimalRe := regexp.MustCompile(`(?i)^(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)`) // capture precision,scale

	for _, meta := range metas {
		imports := lo.Uniq(lo.FilterMap(meta.Fields, func(f Field, _ int) (string, bool) {
			if strings.Contains(f.GoType, ".") {
				pkg := strings.Split(f.GoType, ".")[0]
				switch pkg {
				case "time":
					return "time", true
				default:
					return "", false
				}
			}
			return "", false
		}))

		// compute module package name heuristically: try to load package to get declared name,
		// fall back to last path element if load fails.
		modulePkgName := path.Base(internal.ToolModulePath())
		if pkgs, _ := packages.Load(&packages.Config{Mode: packages.NeedName}, internal.ToolModulePath()); len(pkgs) > 0 {
			if pkgs[0].Name != "" {
				modulePkgName = pkgs[0].Name
			}
		}

		// make a copy of fields so we can annotate ValidatorArgs per-field
		fieldsCopy := make([]Field, len(meta.Fields))
		copy(fieldsCopy, meta.Fields)

		// build validator args based on DBType
		for i := range fieldsCopy {
			f := &fieldsCopy[i]
			db := strings.TrimSpace(strings.ToLower(f.DBType))
			var args []string
			if db != "" {
				if f.GoType == "string" {
					if m := varcharRe.FindStringSubmatch(db); len(m) == 2 {
						// varchar(N) -> MaxLength(N)
						n := m[1]
						args = append(args, fmt.Sprintf("%s.MaxLength(%s)", modulePkgName, n))
					} else if m := decimalRe.FindStringSubmatch(db); len(m) == 3 {
						// decimal(P,S) -> Decimal(P,S) for string backing
						p := m[1]
						s := m[2]
						args = append(args, fmt.Sprintf("%s.Decimal(%s, %s)", modulePkgName, p, s))
					}
				} else {
					// for non-string types
					if m := decimalRe.FindStringSubmatch(db); len(m) == 3 {
						p, _ := strconv.Atoi(m[1])
						s, _ := strconv.Atoi(m[2])
						// if float type use generic Decimal[T]
						switch f.GoType {
						case "float32", "float64":
							args = append(args, fmt.Sprintf("%s.Decimal[%s](%s, %s)", modulePkgName, f.GoType, m[1], m[2]))
						default:
							// integers / unsigned: compute integer max and emit Gte/Lte
							intDigits := p - s
							if intDigits < 1 {
								intDigits = 1
							}
							maxInt := int64(1)
							for k := 0; k < intDigits; k++ {
								maxInt *= 10
							}
							maxInt = maxInt - 1
							switch f.GoType {
							case "int", "int8", "int16", "int32", "int64":
								args = append(args, fmt.Sprintf("%s.Gte[%s](%d)", modulePkgName, f.GoType, -maxInt))
								args = append(args, fmt.Sprintf("%s.Lte[%s](%d)", modulePkgName, f.GoType, maxInt))
							case "uint", "uint8", "uint16", "uint32", "uint64":
								args = append(args, fmt.Sprintf("%s.Gte[%s](%d)", modulePkgName, f.GoType, 0))
								args = append(args, fmt.Sprintf("%s.Lte[%s](%d)", modulePkgName, f.GoType, maxInt))
							}
						}
					}
				}
			}
			if len(args) > 0 {
				// prefix with comma and space to append into template call
				f.ValidatorArgs = ", " + strings.Join(args, ", ")
			} else {
				f.ValidatorArgs = ""
			}
		}

		data := TemplateData{
			PackageName:      strings.ToLower(meta.StructName),
			StructName:       meta.StructName,
			Imports:          imports,
			Fields:           fieldsCopy,
			ModulePath:       internal.ToolModulePath(),
			ModulePkgName:    modulePkgName,
			EntityImportPath: meta.PkgPath,
			GeneratedAt:      time.Now(),
			Version:          computeEntityVersion(meta),
		}

		outputDir := filepath.Join(project.GenPath(), "field", data.PackageName)
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory %s: %w", outputDir, err)
		}
		outputPath := filepath.Join(outputDir, fmt.Sprintf("%s_gen.go", data.PackageName))

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			return fmt.Errorf("failed to execute template for %s: %w", meta.StructName, err)
		}

		formatted, err := format.Source(buf.Bytes())
		if err != nil {
			return fmt.Errorf("failed to format generated code for %s: %w", meta.StructName, err)
		}

		if err := os.WriteFile(outputPath, formatted, 0644); err != nil {
			return fmt.Errorf("failed to write generated file for %s: %w", meta.StructName, err)
		}
		// generation info suppressed in non-verbose mode
	}
	return nil
}

// generateSchemaFromMeta generates schemas from the precomputed entity metadata.
func generateSchemaFromMeta(ctx context.Context, metas []EntityMeta) error {
	project := internal.Current
	if project == nil {
		return fmt.Errorf("project context not initialized")
	}

	adapters, ok := ctx.Value(dbaAdapterKey).([]string)
	if !ok || len(adapters) == 0 {
		return fmt.Errorf("no database adapters are configured or detected")
	}

	funcMap := template.FuncMap{
		"plus1": func(i int) int { return i + 1 },
	}

	tmpl, err := template.New("schema").Funcs(funcMap).Parse(schemaTmpl)
	if err != nil {
		return fmt.Errorf("failed to parse schema template: %w", err)
	}

	for _, adapter := range adapters {
		for _, meta := range metas {
			fields := enrichFieldsForAdapter(meta.Fields, adapter)
			if len(fields) == 0 {
				continue
			}

			data := SchemaTemplateData{
				TableName:   meta.TableName,
				Fields:      fields,
				GeneratedAt: time.Now(),
				Version:     computeEntityVersion(meta),
			}

			outputDir := filepath.Join(project.GenPath(), "schemas", adapter)
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				return fmt.Errorf("failed to create output directory %s: %w", outputDir, err)
			}
			outputPath := filepath.Join(outputDir, fmt.Sprintf("%s_schema.sql", lo.SnakeCase(meta.StructName)))

			var buf bytes.Buffer
			if err := tmpl.Execute(&buf, data); err != nil {
				return fmt.Errorf("failed to execute schema template for %s: %w", meta.StructName, err)
			}
			if err := os.WriteFile(outputPath, buf.Bytes(), 0644); err != nil {
				return fmt.Errorf("failed to write generated schema for %s: %w", meta.StructName, err)
			}
			// generation info suppressed in non-verbose mode
		}
	}

	return nil
}

// enrichFieldsForAdapter clones the base fields and fills DBType/PK warnings for the given adapter.
// This avoids re-parsing AST/types multiple times.
func enrichFieldsForAdapter(base []Field, adapter string) []Field {
	fields := make([]Field, len(base))
	copy(fields, base)
	for i := range fields {
		if fields[i].DBType == "" {
			fields[i].DBType = sqlTypeFor(fields[i].GoType, adapter, driversJSON)
		}
		if fields[i].IsPK {
			_, warning := pkConstraintFor(fields[i].GoType, fields[i].DBType, adapter, driversJSON)
			fields[i].Warning = warning
		}
	}
	return fields
}

func parseFields(pkg *packages.Package, spec *ast.TypeSpec, adapter string) ([]Field, error) {
	// NOTE: adapter is intentionally ignored now; adapter-specific typing happens in enrichFieldsForAdapter.
	_ = adapter
	structType, ok := spec.Type.(*ast.StructType)
	if !ok {
		return nil, nil
	}

	var fields []Field
	for _, field := range structType.Fields.List {
		if len(field.Names) == 0 { // Embedded struct
			var ident *ast.Ident
			switch t := field.Type.(type) {
			case *ast.Ident:
				ident = t
			case *ast.SelectorExpr:
				ident = t.Sel
			}

			if ident != nil && ident.Obj != nil && ident.Obj.Kind == ast.Typ {
				if embeddedSpec, ok := ident.Obj.Decl.(*ast.TypeSpec); ok {
					embeddedFields, err := parseFields(pkg, embeddedSpec, adapter)
					if err != nil {
						return nil, err
					}
					for i := range embeddedFields {
						embeddedFields[i].IsEmbedded = true
					}
					fields = append(fields, embeddedFields...)
				}
			}
			continue
		}

		if !field.Names[0].IsExported() {
			continue // Skip private fields
		}

		// Check if the field is a struct type that should be skipped
		if tv, ok := pkg.TypesInfo.Types[field.Type]; ok {
			if !isSupportedType(tv.Type) {
				if _, ok := tv.Type.Underlying().(*types.Struct); !ok {
					return nil, fmt.Errorf("unsupported field type %s for field %s", tv.Type.String(), field.Names[0].Name)
				}
			}
			if _, ok := tv.Type.Underlying().(*types.Struct); ok {
				// Allow time.Time, but skip other structs
				if tv.Type.String() != "time.Time" {
					continue
				}
			}
		}

		xqlTag := ""
		if field.Tag != nil {
			tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
			xqlTag = tag.Get("xql")
		}

		if xqlTag == "-" {
			continue // Skip ignored fields
		}

		goType := types.ExprString(field.Type)
		// For selector expressions like `time.Time`, we need to get the full type string.
		if se, ok := field.Type.(*ast.SelectorExpr); ok {
			if x, ok := se.X.(*ast.Ident); ok {
				goType = fmt.Sprintf("%s.%s", x.Name, se.Sel.Name)
			}
		}

		entityField := Field{
			GoName: field.Names[0].Name,
			GoType: goType,
			Name:   lo.SnakeCase(field.Names[0].Name),
		}

		parseDirectives(xqlTag, &entityField)

		fields = append(fields, entityField)
	}
	return fields, nil
}

func parseDirectives(tag string, field *Field) {
	directives := strings.Split(tag, ";")
	for _, d := range directives {
		d = strings.TrimSpace(d)
		if d == "" {
			continue
		}

		parts := strings.SplitN(d, ":", 2)
		key := strings.ToLower(parts[0])
		var value string
		if len(parts) > 1 {
			value = parts[1]
		}

		switch key {
		case "pk":
			field.IsPK = true
		case "not null":
			field.IsNotNull = true
		case "unique":
			field.IsUnique = true
		case "index":
			field.IsIndexed = true
		case "name":
			field.Name = value
		case "type":
			field.DBType = value
		case "default":
			field.Default = value
		case "fk":
			fkParts := strings.SplitN(value, ".", 2)
			if len(fkParts) == 2 {
				field.FKTable = fkParts[0]
				field.FKColumn = fkParts[1]
			}
		}
	}
}

// sqlTypeFor returns the SQL type for a given Go type and adapter using the
// parsed drivers JSON (queried via gjson). If no mapping exists, it falls back
// to a sensible default.
func sqlTypeFor(goType string, adapter string, driversJSON []byte) string {
	if len(driversJSON) > 0 {
		path := fmt.Sprintf("%s.typeMapping.%s", adapter, goType)
		if res := gjson.GetBytes(driversJSON, path); res.Exists() {
			return res.String()
		}
	}
	// fallback defaults (conservative)
	switch goType {
	case "int64", "int":
		return "BIGINT"
	case "int32":
		return "INTEGER"
	case "int16":
		return "SMALLINT"
	case "int8":
		return "SMALLINT"
	case "bool":
		if adapter == "mysql" {
			return "TINYINT(1)"
		}
		return "BOOLEAN"
	case "string":
		return "TEXT"
	case "float32":
		return "REAL"
	case "float64":
		if adapter == "postgres" {
			return "DOUBLE PRECISION"
		}
		return "DOUBLE"
	case "time.Time":
		if adapter == "postgres" {
			return "TIMESTAMP WITH TIME ZONE"
		}
		return "DATETIME"
	case "[]byte":
		if adapter == "postgres" {
			return "BYTEA"
		}
		return "BLOB"
	default:
		return "TEXT"
	}
}

// pkConstraintFor returns the PK constraint clause for the given Go type and
// SQL type for the adapter. It normalizes the SQL type, tries exact and family
// fallbacks, and returns an optional warning if PK is used on a discouraged Go type.
func pkConstraintFor(goType string, sqlType string, adapter string, driversJSON []byte) (string, string) {
	if len(driversJSON) == 0 {
		return "", ""
	}
	infoPath := fmt.Sprintf("%s.pk", adapter)
	norm := strings.ToLower(strings.TrimSpace(sqlType))
	if i := strings.Index(norm, "("); i >= 0 {
		norm = strings.TrimSpace(norm[:i])
	}
	if res := gjson.GetBytes(driversJSON, infoPath+"."+norm); res.Exists() {
		v := res.String()
		if goType == "int8" {
			return v, "primary key defined on int8: small integer PKs are discouraged"
		}
		return v, ""
	}
	if res := gjson.GetBytes(driversJSON, infoPath+".integer"); res.Exists() {
		if strings.HasPrefix(norm, "int") || norm == "bigint" || norm == "smallint" || norm == "tinyint" {
			v := res.String()
			if goType == "int8" {
				return v, "primary key defined on int8: small integer PKs are discouraged"
			}
			return v, ""
		}
	}
	return "", ""
}

// computeEntityVersion builds a deterministic fingerprint for an entity based on
// the resolved table name and the exported fields that affect generation.
// It includes each field's GoName, GoType, generated column name (Name), DBType
// and parsed directive flags. Fields are sorted by GoName to avoid churn from
// reordering.
func computeEntityVersion(meta EntityMeta) string {
	type vf struct {
		GoName     string `json:"goName"`
		GoType     string `json:"goType"`
		Name       string `json:"name"`
		DBType     string `json:"dbType"`
		IsPK       bool   `json:"isPK"`
		IsNotNull  bool   `json:"isNotNull"`
		IsUnique   bool   `json:"isUnique"`
		IsIndexed  bool   `json:"isIndexed"`
		Default    string `json:"default"`
		FKTable    string `json:"fkTable"`
		FKColumn   string `json:"fkColumn"`
		IsEmbedded bool   `json:"isEmbedded"`
	}

	vfs := make([]vf, 0, len(meta.Fields))
	for _, f := range meta.Fields {
		vfs = append(vfs, vf{
			GoName:     f.GoName,
			GoType:     f.GoType,
			Name:       f.Name,
			DBType:     f.DBType,
			IsPK:       f.IsPK,
			IsNotNull:  f.IsNotNull,
			IsUnique:   f.IsUnique,
			IsIndexed:  f.IsIndexed,
			Default:    f.Default,
			FKTable:    f.FKTable,
			FKColumn:   f.FKColumn,
			IsEmbedded: f.IsEmbedded,
		})
	}

	// Sort by GoName then GoType to make ordering deterministic and avoid churn due to reordering.
	sort.SliceStable(vfs, func(i, j int) bool {
		if vfs[i].GoName == vfs[j].GoName {
			return vfs[i].GoType < vfs[j].GoType
		}
		return vfs[i].GoName < vfs[j].GoName
	})

	payload := struct {
		Table  string `json:"table"`
		Fields []vf   `json:"fields"`
	}{
		Table:  meta.TableName,
		Fields: vfs,
	}

	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:10]
}

var _ = errors.New

func buildImports(meta EntityMeta) []string {
	imports := lo.Uniq(lo.FilterMap(meta.Fields, func(f Field, _ int) (string, bool) {
		if strings.Contains(f.GoType, ".") {
			pkg := strings.Split(f.GoType, ".")[0]
			switch pkg {
			case "time":
				return "time", true
			default:
				return "", false
			}
		}
		return "", false
	}))
	return imports
}
