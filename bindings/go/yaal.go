// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

// Package yaal compiles Yaal SQL through the shared library.
package yaal

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

//go:embed version.txt
var versionText string

//go:embed native
var nativeFiles embed.FS

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

// Values returns the expanded bind values in order.
// Integral JSON numbers are int64. Other numbers are float64. null is nil.
func (statement Statement) Values() ([]any, error) {
	values := make([]any, len(statement.Parameters))
	for index, parameter := range statement.Parameters {
		decoder := json.NewDecoder(bytes.NewReader(parameter.Value))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		number, ok := value.(json.Number)
		if !ok {
			values[index] = value
			continue
		}
		if integer, err := number.Int64(); err == nil {
			values[index] = integer
			continue
		}
		floatValue, err := number.Float64()
		if err != nil {
			return nil, err
		}
		values[index] = floatValue
	}
	return values, nil
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
	packaged, err := packagedLibrary()
	if err != nil {
		return "", err
	}
	if packaged != "" {
		return packaged, nil
	}
	name := libraryFileName(runtimeID())
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

func runtimeID() string {
	osName := runtime.GOOS
	switch osName {
	case "windows":
		osName = "win"
	case "darwin":
		osName = "osx"
	}
	arch := runtime.GOARCH
	switch arch {
	case "amd64":
		arch = "x64"
	}
	return osName + "-" + arch
}

func libraryFileName(rid string) string {
	switch {
	case strings.HasPrefix(rid, "win-"):
		return "yaal_ffi.dll"
	case strings.HasPrefix(rid, "osx-"):
		return "libyaal_ffi.dylib"
	default:
		return "libyaal_ffi.so"
	}
}

func packagedLibrary() (string, error) {
	rid := runtimeID()
	name := libraryFileName(rid)
	data, err := nativeFiles.ReadFile(path.Join("native", rid, name))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	if len(data) == 0 {
		return "", nil
	}
	version := strings.TrimSpace(versionText)
	if version == "" {
		version = "dev"
	}
	dir := filepath.Join(os.TempDir(), "yaal", version)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	dest := filepath.Join(dir, name)
	info, statErr := os.Stat(dest)
	if statErr == nil && info.Mode().IsRegular() && info.Size() == int64(len(data)) {
		return dest, nil
	}
	partial, err := os.CreateTemp(dir, "yaal-*")
	if err != nil {
		return "", err
	}
	partialName := partial.Name()
	written := false
	defer func() {
		if !written {
			os.Remove(partialName)
		}
	}()
	if _, err := partial.Write(data); err != nil {
		partial.Close()
		return "", err
	}
	if err := partial.Chmod(0o755); err != nil {
		partial.Close()
		return "", err
	}
	if err := partial.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(dest); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	if err := os.Rename(partialName, dest); err != nil {
		return "", err
	}
	written = true
	return dest, nil
}
