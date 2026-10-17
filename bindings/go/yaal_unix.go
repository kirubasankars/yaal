// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

//go:build unix

package yaal

/*
#cgo linux LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdlib.h>

typedef char *(*yaal_compile_fn)(const char *, const char *, const char *);
typedef void (*yaal_free_fn)(char *);

static void *yaal_open(const char *path) { return dlopen(path, RTLD_NOW | RTLD_LOCAL); }
static void *yaal_sym(void *handle, const char *name) { return dlsym(handle, name); }
static const char *yaal_dlerror(void) { return dlerror(); }
static char *yaal_call(void *fn, const char *sql, const char *params, const char *placeholder) {
    return ((yaal_compile_fn)fn)(sql, params, placeholder);
}
static void yaal_release(void *fn, char *ptr) { ((yaal_free_fn)fn)(ptr); }
*/
import "C"
import (
	"errors"
	"unsafe"
)

type unixLib struct {
	compileFn unsafe.Pointer
	freeFn    unsafe.Pointer
}

func openLibrary() (nativeAPI, error) {
	path, err := libraryPath()
	if err != nil {
		return nil, err
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	handle := C.yaal_open(cpath)
	if handle == nil {
		return nil, errors.New(C.GoString(C.yaal_dlerror()))
	}
	compileName := C.CString("yaal_compile")
	freeName := C.CString("yaal_free")
	defer C.free(unsafe.Pointer(compileName))
	defer C.free(unsafe.Pointer(freeName))
	compileSym := C.yaal_sym(handle, compileName)
	if compileSym == nil {
		return nil, errors.New(C.GoString(C.yaal_dlerror()))
	}
	freeSym := C.yaal_sym(handle, freeName)
	if freeSym == nil {
		return nil, errors.New(C.GoString(C.yaal_dlerror()))
	}
	return unixLib{compileFn: compileSym, freeFn: freeSym}, nil
}

func (lib unixLib) compile(sql, params, placeholder string) (string, error) {
	csql := C.CString(sql)
	cparams := C.CString(params)
	cplaceholder := C.CString(placeholder)
	defer C.free(unsafe.Pointer(csql))
	defer C.free(unsafe.Pointer(cparams))
	defer C.free(unsafe.Pointer(cplaceholder))
	ptr := C.yaal_call(lib.compileFn, csql, cparams, cplaceholder)
	if ptr == nil {
		return "", errors.New("yaal_compile returned null")
	}
	defer C.yaal_release(lib.freeFn, ptr)
	return C.GoString(ptr), nil
}
