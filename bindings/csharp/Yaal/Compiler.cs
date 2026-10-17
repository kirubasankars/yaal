// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Yaal;

public sealed class CompileException : Exception
{
    public CompileException(string message) : base(message) { }
}

public sealed record Compiled(
    [property: JsonPropertyName("statements")] IReadOnlyList<CompiledStatement> Statements);

public sealed record CompiledStatement(
    [property: JsonPropertyName("sql")] string Sql,
    [property: JsonPropertyName("parameters")] IReadOnlyList<BoundParameter> Parameters);

public sealed record BoundParameter(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("type")] string Type,
    [property: JsonPropertyName("value")] JsonElement Value);

public static class Compiler
{
    public static Compiled Compile(string sql, object? parameters = null, string placeholder = "question")
    {
        ArgumentNullException.ThrowIfNull(sql);
        placeholder = string.IsNullOrEmpty(placeholder) ? "question" : placeholder;
        if (sql.Contains('\0') || placeholder.Contains('\0'))
            throw new CompileException("sql and placeholder must not contain a NUL character");
        var paramsJson = parameters is null ? "{}" : JsonSerializer.Serialize(parameters);
        var raw = Invoke(sql, paramsJson, placeholder);
        using var document = JsonDocument.Parse(raw);
        if (!document.RootElement.TryGetProperty("statements", out _))
        {
            var message = document.RootElement.TryGetProperty("error", out var error)
                ? error.GetString() ?? raw
                : raw;
            throw new CompileException(message);
        }
        return JsonSerializer.Deserialize<Compiled>(raw)
            ?? throw new CompileException("compiler returned no result");
    }

    static string Invoke(string sql, string paramsJson, string placeholder)
    {
        var sqlPtr = Marshal.StringToCoTaskMemUTF8(sql);
        var paramsPtr = Marshal.StringToCoTaskMemUTF8(paramsJson);
        var placeholderPtr = Marshal.StringToCoTaskMemUTF8(placeholder);
        try
        {
            var result = Native.compile(sqlPtr, paramsPtr, placeholderPtr);
            if (result == IntPtr.Zero)
                throw new CompileException("yaal_compile returned null");
            try
            {
                return Marshal.PtrToStringUTF8(result)
                    ?? throw new CompileException("compiler returned an empty result");
            }
            finally
            {
                Native.free(result);
            }
        }
        finally
        {
            Marshal.FreeCoTaskMem(sqlPtr);
            Marshal.FreeCoTaskMem(paramsPtr);
            Marshal.FreeCoTaskMem(placeholderPtr);
        }
    }

    static class Native
    {
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate IntPtr CompileFn(IntPtr sql, IntPtr paramsJson, IntPtr placeholder);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void FreeFn(IntPtr ptr);

        static readonly object Gate = new();
        static CompileFn? compileFn;
        static FreeFn? freeFn;

        internal static CompileFn compile => Ready().compile;
        internal static FreeFn free => Ready().free;

        static (CompileFn compile, FreeFn free) Ready()
        {
            lock (Gate)
            {
                if (compileFn is not null && freeFn is not null)
                    return (compileFn, freeFn);
                var library = Load();
                compileFn = Marshal.GetDelegateForFunctionPointer<CompileFn>(
                    NativeLibrary.GetExport(library, "yaal_compile"));
                freeFn = Marshal.GetDelegateForFunctionPointer<FreeFn>(
                    NativeLibrary.GetExport(library, "yaal_free"));
                return (compileFn, freeFn);
            }
        }

        static IntPtr Load()
        {
            var path = LibraryPath();
            try
            {
                return NativeLibrary.Load(path);
            }
            catch (Exception ex)
            {
                throw new CompileException($"failed to load {path}: {ex.Message}");
            }
        }

        static string LibraryPath()
        {
            var env = Environment.GetEnvironmentVariable("YAAL_LIB");
            if (!string.IsNullOrEmpty(env))
                return env;
            var name = OperatingSystem.IsWindows() ? "yaal_ffi.dll"
                : OperatingSystem.IsMacOS() ? "libyaal_ffi.dylib"
                : "libyaal_ffi.so";
            foreach (var start in new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory })
            {
                var dir = new DirectoryInfo(start);
                while (dir is not null)
                {
                    foreach (var profile in new[] { "debug", "release" })
                    {
                        var candidate = Path.Combine(dir.FullName, "target", profile, name);
                        if (File.Exists(candidate))
                            return candidate;
                    }
                    dir = dir.Parent;
                }
            }
            throw new CompileException("yaal shared library not found; set YAAL_LIB");
        }
    }
}
