// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

// Package yaal compiles Yaal SQL through the shared library.
package yaal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// CompileError is returned when the compiler rejects the SQL or the parameters.
type CompileError struct {
	Message string
}

func (e *CompileError) Error() string { return e.Message }

// Compiled is the compiler result. Parameter values keep their JSON form.
type Compiled struct {
	Statements []Statement `json:"statements"`
}

// Statement is one compiled SQL statement and its ordered binds.
type Statement struct {
	SQL        string      `json:"sql"`
	Parameters []Parameter `json:"parameters"`
}

// Parameter is one bind. Value is the raw JSON value.
type Parameter struct {
	Name  string          `json:"name"`
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

type nativeAPI interface {
	compile(sql, params, placeholder string) (string, error)
}

var (
	loadOnce sync.Once
	loaded   nativeAPI
	loadErr  error
)

// Compile returns the statements for sql. params is encoded as JSON.
// A nil params value is an empty object. placeholder is "question" or "numbered".
func Compile(sql string, params any, placeholder string) (*Compiled, error) {
	if placeholder == "" {
		placeholder = "question"
	}
	if strings.IndexByte(sql, 0) >= 0 || strings.IndexByte(placeholder, 0) >= 0 {
		return nil, &CompileError{Message: "sql and placeholder must not contain a NUL character"}
	}
	var raw []byte
	var err error
	if params == nil {
		raw = []byte("{}")
	} else {
		raw, err = json.Marshal(params)
		if err != nil {
			return nil, err
		}
	}
	text, err := call(sql, string(raw), placeholder)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		Error      string          `json:"error"`
		Statements json.RawMessage `json:"statements"`
	}
	if err := json.Unmarshal([]byte(text), &envelope); err != nil {
		return nil, err
	}
	if len(envelope.Statements) == 0 {
		if envelope.Error != "" {
			return nil, &CompileError{Message: envelope.Error}
		}
		return nil, &CompileError{Message: text}
	}
	var compiled Compiled
	if err := json.Unmarshal([]byte(text), &compiled); err != nil {
		return nil, err
	}
	return &compiled, nil
}

func call(sql, params, placeholder string) (string, error) {
	loadOnce.Do(func() {
		loaded, loadErr = openLibrary()
	})
	if loadErr != nil {
		return "", loadErr
	}
	return loaded.compile(sql, params, placeholder)
}

func libraryPath() (string, error) {
	if env := os.Getenv("YAAL_LIB"); env != "" {
		return env, nil
	}
	name := "libyaal_ffi.so"
	switch runtime.GOOS {
	case "windows":
		name = "yaal_ffi.dll"
	case "darwin":
		name = "libyaal_ffi.dylib"
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		for _, profile := range []string{"debug", "release"} {
			candidate := filepath.Join(dir, "target", profile, name)
			info, err := os.Stat(candidate)
			if err == nil && !info.IsDir() {
				return candidate, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("yaal shared library not found; set YAAL_LIB")
}
