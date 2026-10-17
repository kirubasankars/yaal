// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package yaal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sqlText = `--(id, active)--
select * from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
`

const partialText = `--(id, active)--
select * from users u
where optional(u.id = {{id}} and u.active = {{active}})
`

func TestCompile(t *testing.T) {
	compiled, err := Compile(sqlText, map[string]any{"id": 1}, "question")
	if err != nil {
		t.Fatal(err)
	}
	if len(compiled.Statements) != 1 {
		t.Fatalf("statements: %d", len(compiled.Statements))
	}
	got := strings.Join(strings.Fields(compiled.Statements[0].SQL), " ")
	want := "select * from users u where (u.id = ?)"
	if got != want {
		t.Fatalf("sql %q", got)
	}
	params := compiled.Statements[0].Parameters
	if len(params) != 1 || params[0].Name != "id" || params[0].Type != "integer" || string(params[0].Value) != "1" {
		t.Fatalf("params %+v", params)
	}

	numbered, err := Compile(sqlText, map[string]any{"id": 1}, "numbered")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(numbered.Statements[0].SQL, "$1") || strings.Contains(numbered.Statements[0].SQL, "?") {
		t.Fatalf("numbered sql %q", numbered.Statements[0].SQL)
	}
}

func TestFixtureCases(t *testing.T) {
	path := filepath.Join("..", "..", "tests", "fixtures", "sql_compile", "cases.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name                string          `json:"name"`
		SQL                 string          `json:"sql"`
		Params              json.RawMessage `json:"params"`
		ExpectSQL           string          `json:"expect_sql"`
		ExpectParamNames    []string        `json:"expect_param_names"`
		ExpectErrorContains string          `json:"expect_error_contains"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 100 {
		t.Fatalf("fixture cases: %d", len(cases))
	}
	for _, item := range cases {
		t.Run(item.Name, func(t *testing.T) {
			compiled, err := Compile(item.SQL, item.Params, "question")
			if item.ExpectErrorContains != "" {
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(item.ExpectErrorContains)) {
					t.Fatalf("got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(compiled.Statements) != 1 {
				t.Fatalf("statements: %d", len(compiled.Statements))
			}
			got := strings.Join(strings.Fields(compiled.Statements[0].SQL), " ")
			want := strings.Join(strings.Fields(item.ExpectSQL), " ")
			if got != want {
				t.Fatalf("sql\n got %s\n exp %s", got, want)
			}
			var names []string
			for _, param := range compiled.Statements[0].Parameters {
				names = append(names, param.Name)
			}
			if names == nil {
				names = []string{}
			}
			expect := item.ExpectParamNames
			if expect == nil {
				expect = []string{}
			}
			if strings.Join(names, ",") != strings.Join(expect, ",") {
				t.Fatalf("names %v exp %v", names, expect)
			}
		})
	}
}

func TestPartialOptional(t *testing.T) {
	_, err := Compile(partialText, map[string]any{"id": 1}, "")
	var compileErr *CompileError
	if err == nil || !errorsAs(err, &compileErr) || !strings.Contains(strings.ToLower(err.Error()), "partial") {
		t.Fatalf("got %v", err)
	}
}

func errorsAs(err error, target **CompileError) bool {
	compileErr, ok := err.(*CompileError)
	if ok {
		*target = compileErr
	}
	return ok
}
