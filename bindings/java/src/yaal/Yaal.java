// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package yaal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class Yaal {
    private Yaal() {}

    public record Parameter(String name, String type, Object value) {}

    public record Statement(String sql, List<Parameter> parameters) {
        public List<Object> values() {
            List<Object> values = new ArrayList<>();
            for (Parameter parameter : parameters) {
                values.add(parameter.value());
            }
            return Collections.unmodifiableList(values);
        }
    }

    public record Compiled(List<Statement> statements) {}

    public static Compiled compile(String sql, Map<String, ?> params, String placeholder) throws CompileException {
        if (sql == null) {
            throw new CompileException("sql is null");
        }
        if (placeholder == null || placeholder.isEmpty()) {
            placeholder = "question";
        }
        if (sql.indexOf('\0') >= 0 || placeholder.indexOf('\0') >= 0) {
            throw new CompileException("sql and placeholder must not contain a NUL character");
        }
        String paramsJson = Json.write(params == null ? Map.of() : params);
        Object parsed = Json.read(invoke(sql, paramsJson, placeholder));
        if (!(parsed instanceof Map<?, ?> root)) {
            throw new CompileException("compiler returned " + parsed);
        }
        Object statements = root.get("statements");
        if (!(statements instanceof List<?> list)) {
            Object error = root.get("error");
            throw new CompileException(error == null ? String.valueOf(root) : String.valueOf(error));
        }
        List<Statement> compiled = new ArrayList<>();
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> statement)) {
                throw new CompileException("statement is not an object");
            }
            Object sqlText = statement.get("sql");
            Object parameters = statement.get("parameters");
            if (!(sqlText instanceof String) || !(parameters instanceof List<?> binds)) {
                throw new CompileException("statement is missing sql or parameters");
            }
            List<Parameter> values = new ArrayList<>();
            for (Object bind : binds) {
                if (!(bind instanceof Map<?, ?> parameter)) {
                    throw new CompileException("parameter is not an object");
                }
                values.add(new Parameter(
                        String.valueOf(parameter.get("name")),
                        String.valueOf(parameter.get("type")),
                        parameter.get("value")));
            }
            compiled.add(new Statement((String) sqlText, List.copyOf(values)));
        }
        return new Compiled(List.copyOf(compiled));
    }

    private static String invoke(String sql, String paramsJson, String placeholder) throws CompileException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = (MemorySegment) Native.compile.invoke(
                    arena.allocateFrom(sql),
                    arena.allocateFrom(paramsJson),
                    arena.allocateFrom(placeholder));
            if (result.equals(MemorySegment.NULL)) {
                throw new CompileException("yaal_compile returned null");
            }
            String text = result.reinterpret(Long.MAX_VALUE).getString(0);
            Native.free.invoke(result);
            return text;
        } catch (CompileException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new CompileException(ex.getMessage() == null ? ex.toString() : ex.getMessage());
        }
    }

    private static final class Native {
        static final MethodHandle compile;
        static final MethodHandle free;

        static {
            try {
                Linker linker = Linker.nativeLinker();
                SymbolLookup lookup = SymbolLookup.libraryLookup(libraryPath(), Arena.global());
                compile = linker.downcallHandle(
                        lookup.find("yaal_compile").orElseThrow(),
                        FunctionDescriptor.of(
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS));
                free = linker.downcallHandle(
                        lookup.find("yaal_free").orElseThrow(),
                        FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new ExceptionInInitializerError(ex);
            }
        }

        static Path libraryPath() throws CompileException {
            String env = System.getenv("YAAL_LIB");
            if (env != null && !env.isEmpty()) {
                return Path.of(env);
            }
            Path packaged = packagedLibrary();
            if (packaged != null) {
                return packaged;
            }
            Path built = builtLibrary();
            if (built != null) {
                return built;
            }
            throw new CompileException("yaal shared library not found; set YAAL_LIB");
        }

        static Path packagedLibrary() throws CompileException {
            String runtime = runtimeId();
            String name = libraryFileName(runtime);
            InputStream in = Yaal.class.getResourceAsStream("/yaal/native/" + runtime + "/" + name);
            if (in == null) {
                return null;
            }
            try (in) {
                return extract(in, name);
            } catch (IOException ex) {
                throw new CompileException(ex.getMessage() == null ? ex.toString() : ex.getMessage());
            }
        }

        static Path extract(InputStream in, String name) throws IOException {
            String version = Yaal.class.getPackage().getImplementationVersion();
            if (version == null || version.isEmpty()) {
                version = "dev";
            }
            Path dir = Path.of(System.getProperty("java.io.tmpdir"), "yaal", version);
            Files.createDirectories(dir);
            Path dest = dir.resolve(name);
            if (Files.isRegularFile(dest) && Files.size(dest) > 0) {
                return dest;
            }
            Path partial = Files.createTempFile(dir, "yaal", ".partial");
            try {
                Files.copy(in, partial, StandardCopyOption.REPLACE_EXISTING);
                try {
                    Files.move(
                            partial,
                            dest,
                            StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException ex) {
                    Files.move(partial, dest, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(partial);
            }
            try {
                Set<PosixFilePermission> perms = new HashSet<>(Files.getPosixFilePermissions(dest));
                perms.add(PosixFilePermission.OWNER_READ);
                perms.add(PosixFilePermission.OWNER_EXECUTE);
                Files.setPosixFilePermissions(dest, perms);
            } catch (UnsupportedOperationException ex) {
                // Windows has no POSIX permissions.
            }
            return dest;
        }

        static Path builtLibrary() {
            String name = libraryFileName(runtimeId());
            Path dir = Path.of("").toAbsolutePath();
            while (dir != null) {
                for (String profile : List.of("debug", "release")) {
                    Path candidate = dir.resolve("target").resolve(profile).resolve(name);
                    if (Files.isRegularFile(candidate)) {
                        return candidate;
                    }
                }
                dir = dir.getParent();
            }
            return null;
        }

        static String runtimeId() {
            String os = System.getProperty("os.name", "").toLowerCase();
            String arch = System.getProperty("os.arch", "").toLowerCase();
            String osId = os.contains("win") ? "win" : os.contains("mac") ? "osx" : "linux";
            String archId = switch (arch) {
                case "amd64", "x86_64" -> "x64";
                case "aarch64", "arm64" -> "arm64";
                default -> arch;
            };
            return osId + "-" + archId;
        }

        static String libraryFileName(String runtime) {
            if (runtime.startsWith("win-")) {
                return "yaal_ffi.dll";
            }
            if (runtime.startsWith("osx-")) {
                return "libyaal_ffi.dylib";
            }
            return "libyaal_ffi.so";
        }
    }
}
