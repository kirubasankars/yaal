// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

//go:build windows

package yaal

import (
	"errors"
	"syscall"
	"unsafe"
)

type windowsLib struct {
	compile *syscall.Proc
	free    *syscall.Proc
}

func openLibrary() (nativeAPI, error) {
	path, err := libraryPath()
	if err != nil {
		return nil, err
	}
	dll, err := syscall.LoadDLL(path)
	if err != nil {
		return nil, err
	}
	compile, err := dll.FindProc("yaal_compile")
	if err != nil {
		return nil, err
	}
	free, err := dll.FindProc("yaal_free")
	if err != nil {
		return nil, err
	}
	return windowsLib{compile: compile, free: free}, nil
}

func (lib windowsLib) compile(sql, params, placeholder string) (string, error) {
	sqlPtr, err := syscall.BytePtrFromString(sql)
	if err != nil {
		return "", err
	}
	paramsPtr, err := syscall.BytePtrFromString(params)
	if err != nil {
		return "", err
	}
	placeholderPtr, err := syscall.BytePtrFromString(placeholder)
	if err != nil {
		return "", err
	}
	result, _, _ := lib.compile.Call(
		uintptr(unsafe.Pointer(sqlPtr)),
		uintptr(unsafe.Pointer(paramsPtr)),
		uintptr(unsafe.Pointer(placeholderPtr)),
	)
	if result == 0 {
		return "", errors.New("yaal_compile returned null")
	}
	text := goString(result)
	lib.free.Call(result)
	return text, nil
}

func goString(ptr uintptr) string {
	var bytes []byte
	for {
		b := *(*byte)(unsafe.Pointer(ptr))
		if b == 0 {
			return string(bytes)
		}
		bytes = append(bytes, b)
		ptr++
	}
}
