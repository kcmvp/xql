package xql

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kcmvp/xql/cmd/internal"
	"github.com/stretchr/testify/require"
)

func compareGoFileWithJSON(t *testing.T, goFilePath, jsonFilePath string) {
	// Read the generated Go file
	content, err := os.ReadFile(goFilePath)
	require.NoError(t, err)

	// Parse the Go file
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "", content, parser.ParseComments)
	require.NoError(t, err)

	// Extract the fields from the var block
	fields := make(map[string]string)
	ast.Inspect(node, func(n ast.Node) bool {
		if vs, ok := n.(*ast.ValueSpec); ok {
			for _, name := range vs.Names {
				if len(vs.Values) > 0 {
					// Get the source code of the value expression
					start := vs.Values[0].Pos() - 1
					end := vs.Values[0].End() - 1
					if int(start) < len(content) && int(end) < len(content) {
						fields[name.Name] = string(content[start:end])
					}
				}
			}
		}
		return true
	})

	// Read the JSON file
	jsonContent, err := os.ReadFile(jsonFilePath)
	require.NoError(t, err)

	// Unmarshal the JSON file
	var expectedFields map[string]string
	if len(jsonContent) == 0 {
		expectedFields = map[string]string{}
	} else {
		err = json.Unmarshal(jsonContent, &expectedFields)
		require.NoError(t, err)
	}

	// Filter out view wrapper variables (View*) from actual fields since
	// generator may emit both persistent field variables and view wrappers.
	filtered := make(map[string]string, len(fields))
	for k, v := range fields {
		if strings.HasPrefix(k, "View") {
			continue
		}
		filtered[k] = v
	}

	// Compare the fields (persistent-only)
	require.Equal(t, expectedFields, filtered)
}

// compareInMemoryWithJSON compares an in-memory generated Go file (from map) with expected JSON field list
func compareInMemoryWithJSON(t *testing.T, generated map[string][]byte, goPath, jsonPath string) {
	content, ok := generated[goPath]
	require.True(t, ok, "generated content missing for %s", goPath)

	// Parse the Go content
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "", content, parser.ParseComments)
	require.NoError(t, err)

	fields := make(map[string]string)
	ast.Inspect(node, func(n ast.Node) bool {
		if vs, ok := n.(*ast.ValueSpec); ok {
			for _, name := range vs.Names {
				if len(vs.Values) > 0 {
					start := vs.Values[0].Pos() - 1
					end := vs.Values[0].End() - 1
					if int(start) < len(content) && int(end) < len(content) {
						fields[name.Name] = string(content[start:end])
					}
				}
			}
		}
		return true
	})

	jsonContent, err := os.ReadFile(jsonPath)
	require.NoError(t, err)

	var expected map[string]string
	if len(jsonContent) == 0 {
		expected = map[string]string{}
	} else {
		require.NoError(t, json.Unmarshal(jsonContent, &expected))
	}

	filtered := make(map[string]string, len(fields))
	for k, v := range fields {
		if strings.HasPrefix(k, "View") {
			continue
		}
		filtered[k] = v
	}

	require.Equal(t, expected, filtered)
}

// compareInMemoryWithFiles compares in-memory generated content with on-disk file content using cleanSQL
func compareInMemoryWithFiles(t *testing.T, generated map[string][]byte, generatedPath, expectedFile string) {
	content, ok := generated[generatedPath]
	require.True(t, ok, "generated content missing for %s", generatedPath)
	expectedContent, err := os.ReadFile(expectedFile)
	require.NoError(t, err)
	if cleanSQL(string(expectedContent)) != cleanSQL(string(content)) {
		t.Logf("Generated content for %s differs:\n%s", generatedPath, string(content))
	}
	require.Equal(t, cleanSQL(string(expectedContent)), cleanSQL(string(content)))
}

func cleanSQL(content string) string {
	lines := strings.Split(content, "\n")
	var cleanedLines []string
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine != "" && !strings.HasPrefix(trimmedLine, "--") {
			cleanedLines = append(cleanedLines, trimmedLine)
		}
	}
	return strings.Join(cleanedLines, "\n")
}

func TestGeneration(t *testing.T) {
	// Ensure the project is initialized
	require.NotNil(t, internal.Current, "internal.Current should be initialized")

	// Create a context with database adapters
	ctx := context.WithValue(context.Background(), dbaAdapterKey, []string{"sqlite", "postgres", "mysql"})
	filter := func(e internal.EntityInfo) bool {
		return e.TypeSpec != nil && e.TypeSpec.Name != nil && !strings.HasPrefix(e.TypeSpec.Name.Name, "Negative")
	}
	ctx = context.WithValue(ctx, entityFilterKey, filter)

	// Generate to memory to avoid touching disk in tests
	generated, err := generateToMemory(ctx)
	require.NoError(t, err)

	testDataDir := filepath.Join(internal.Current.Root, "testdata")

	// Verify the output for Account fields
	compareInMemoryWithJSON(t, generated,
		filepath.Join(internal.Current.GenPath(), "field", "account", "account_gen.go"),
		filepath.Join(testDataDir, "account_fields.json"),
	)

	// Verify the output for Order fields
	compareInMemoryWithJSON(t, generated,
		filepath.Join(internal.Current.GenPath(), "field", "order", "order_gen.go"),
		filepath.Join(testDataDir, "order_fields.json"),
	)

	// Also verify all generated field packages have matching testdata JSON
	genFieldDir := filepath.Join(internal.Current.GenPath(), "field")
	if entries, err := os.ReadDir(genFieldDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			pkg := e.Name()
			goFile := filepath.Join(genFieldDir, pkg, pkg+"_gen.go")
			jsonFile := filepath.Join(testDataDir, pkg+"_fields.json")
			// ensure files exist and match; compareInMemoryWithJSON will assert
			compareInMemoryWithJSON(t, generated, goFile, jsonFile)
		}
	} else {
		t.Fatalf("failed to read generated field dir: %v", err)
	}

	// Verify the output for schemas
	for _, db := range []string{"sqlite", "postgres", "mysql"} {
		compareInMemoryWithFiles(t, generated,
			filepath.Join(internal.Current.GenPath(), "schemas", db, "account_schema.sql"),
			filepath.Join(testDataDir, "schemas", db, "account_schema.sql"),
		)
		compareInMemoryWithFiles(t, generated,
			filepath.Join(internal.Current.GenPath(), "schemas", db, "order_schema.sql"),
			filepath.Join(testDataDir, "schemas", db, "order_schema.sql"),
		)
	}
	t.Log("test finished")
}

func TestNegativeGeneration(t *testing.T) {
	// Ensure the project is initialized
	require.NotNil(t, internal.Current, "internal.Current should be initialized")

	all := internal.Current.StructsImplementEntity()
	negatives := make([]internal.EntityInfo, 0)
	for _, e := range all {
		if e.TypeSpec == nil || e.TypeSpec.Name == nil {
			continue
		}
		if strings.HasPrefix(e.TypeSpec.Name.Name, "Negative") {
			negatives = append(negatives, e)
		}
	}
	if len(negatives) == 0 {
		t.Skip("no Negative* entities found")
	}

	for _, e := range negatives {
		e := e
		name := e.TypeSpec.Name.Name
		t.Run(name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), dbaAdapterKey, []string{"sqlite", "postgres", "mysql"})
			// Limit generation to this one entity so we can verify every negative case independently.
			ctx = context.WithValue(ctx, entityFilterKey, []string{name})

			err := generate(ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), "unsupported field type")
		})
	}
}
