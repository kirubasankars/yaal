# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

.PHONY: test compile bindings docs ci nuget pypi

test:
	cargo test --workspace
	$(MAKE) bindings

ci: test
	cargo fmt --all -- --check

TARGET_DIR := $(if $(CARGO_TARGET_DIR),$(CARGO_TARGET_DIR),$(CURDIR)/target)
LIB := $(TARGET_DIR)/debug/libyaal_ffi.so
JAVA ?= java
JAVAC ?= $(shell if [ -n "$$JAVA_HOME" ] && [ -x "$$JAVA_HOME/bin/javac" ]; then echo "$$JAVA_HOME/bin/javac"; else command -v javac 2>/dev/null || ls /usr/lib/jvm/*/bin/javac 2>/dev/null | head -1; fi)

bindings:
	cargo build -p yaal-ffi
	YAAL_LIB="$(LIB)" python3 bindings/python/test_yaal.py
	cd bindings/go && YAAL_LIB="$(LIB)" go test .
	YAAL_LIB="$(LIB)" dotnet run --project bindings/csharp/Yaal.Tests/Yaal.Tests.csproj --nologo
	@test -n "$(JAVAC)" && test -x "$(JAVAC)" || { echo "javac not found. Install a JDK, for example java-25-openjdk-devel, or set JAVA_HOME."; exit 1; }
	mkdir -p bindings/java/build
	"$(JAVAC)" --release 22 -d bindings/java/build bindings/java/src/yaal/*.java bindings/java/test/yaal/*.java
	YAAL_LIB="$(LIB)" $(JAVA) --enable-native-access=ALL-UNNAMED -cp bindings/java/build yaal.YaalTest

compile:
	cargo run -p yaal-cli -- compile $(ARGS)

docs:
	python3 docs/serve.py

nuget:
	dotnet pack bindings/csharp/Yaal/Yaal.csproj -c Release -o artifacts -p:ContinuousIntegrationBuild=true

pypi:
	cargo build -p yaal-ffi --release
	@python3 -c "import build, wheel" || { echo "python3 -m pip install build wheel"; exit 1; }
	python3 bindings/python/pack.py
