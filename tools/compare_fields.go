package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func extractFields(goFile string) (map[string]string, error) {
	b, err := ioutil.ReadFile(goFile)
	if err != nil {
		return nil, err
	}
	n := string(b)
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "", n, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	ast.Inspect(node, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range vs.Names {
			if len(vs.Values) > i {
				start := vs.Values[i].Pos() - 1
				end := vs.Values[i].End() - 1
				if int(start) < len(b) && int(end) <= len(b) {
					expr := string(b[start:end])
					out[name.Name] = strings.TrimSpace(expr)
				}
			}
		}
		return true
	})
	return out, nil
}

func main() {
	root := "sample/gen/field"
	matches, err := filepath.Glob(filepath.Join(root, "*", "*_gen.go"))
	if err != nil {
		fmt.Println("glob error:", err)
		os.Exit(1)
	}
	if len(matches) == 0 {
		fmt.Println("no generated files found")
		os.Exit(0)
	}
	var anyDiff bool
	for _, gen := range matches {
		pkg := filepath.Base(filepath.Dir(gen))
		target := filepath.Join("testdata", pkg+"_fields.json")
		fmt.Printf("Checking %s -> %s\n", gen, target)
		actual, err := extractFields(gen)
		if err != nil {
			fmt.Printf("  error parsing generated file: %v\n", err)
			anyDiff = true
			continue
		}
		// read expected
		exists := true
		expB, err := ioutil.ReadFile(target)
		if err != nil {
			exists = false
		}
		var expected map[string]string
		if exists && len(expB) > 0 {
			if err := json.Unmarshal(expB, &expected); err != nil {
				fmt.Printf("  failed to parse %s: %v\n", target, err)
				anyDiff = true
				continue
			}
		} else {
			// empty or missing: auto-write expected from actual
			fmt.Printf("  expected file missing or empty — creating %s\n", target)
			outB, _ := json.MarshalIndent(actual, "", "  ")
			ioutil.WriteFile(target, outB, 0644)
			fmt.Printf("  wrote %d entries\n", len(actual))
			continue
		}
		// compare keys
		ok := true
		for k, v := range actual {
			if expv, found := expected[k]; !found {
				fmt.Printf("  MISSING key in expected: %s\n", k)
				ok = false
				anyDiff = true
			} else if strings.TrimSpace(expv) != strings.TrimSpace(v) {
				fmt.Printf("  VALUE MISMATCH for %s:\n    expected: %s\n    actual:   %s\n", k, expv, v)
				ok = false
				anyDiff = true
			}
		}
		for k := range expected {
			if _, found := actual[k]; !found {
				fmt.Printf("  EXTRA key in expected not in actual: %s\n", k)
				ok = false
				anyDiff = true
			}
		}
		if ok {
			fmt.Printf("  OK: %d entries match\n", len(actual))
		}
	}
	if anyDiff {
		fmt.Println("Differences found or fixed (see above)")
		os.Exit(2)
	}
	fmt.Println("All generated field files match testdata *_fields.json")
}
