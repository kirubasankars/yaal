// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
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
        if (sql is null)
            throw new ArgumentNullException(nameof(sql));
        placeholder = string.IsNullOrEmpty(placeholder) ? "question" : placeholder;
        if (sql.IndexOf('\0') >= 0 || placeholder.IndexOf('\0') >= 0)
            throw new CompileException("sql and placeholder must not contain a NUL character");
        var paramsJson = parameters is null ? "{}" : JsonSerializer.Serialize(parameters);
        var raw = Invoke(sql, paramsJson, placeholder);
        using (var document = JsonDocument.Parse(raw))
        {
            if (!document.RootElement.TryGetProperty("statements", out _))
            {
                var message = document.RootElement.TryGetProperty("error", out var error)
                    ? error.GetString() ?? raw
                    : raw;
                throw new CompileException(message);
            }
        }
        return JsonSerializer.Deserialize<Compiled>(raw)
            ?? throw new CompileException("compiler returned no result");
    }

    static string Invoke(string sql, string paramsJson, string placeholder)
    {
        var sqlPtr = AllocUtf8(sql);
        var paramsPtr = AllocUtf8(paramsJson);
        var placeholderPtr = AllocUtf8(placeholder);
        try
        {
            var result = Native.compile(sqlPtr, paramsPtr, placeholderPtr);
            if (result == IntPtr.Zero)
                throw new CompileException("yaal_compile returned null");
            try
            {
                return ReadUtf8(result);
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

    static IntPtr AllocUtf8(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var ptr = Marshal.AllocCoTaskMem(bytes.Length + 1);
        Marshal.Copy(bytes, 0, ptr, bytes.Length);
        Marshal.WriteByte(ptr, bytes.Length, 0);
        return ptr;
    }

    static string ReadUtf8(IntPtr ptr)
    {
        var length = 0;
        while (Marshal.ReadByte(ptr, length) != 0)
            length++;
        if (length == 0)
            return "";
        var bytes = new byte[length];
        Marshal.Copy(ptr, bytes, 0, length);
        return Encoding.UTF8.GetString(bytes);
    }

    static class Native
    {
        const int RtldNow = 2;

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
                var library = Load(LibraryPath());
                compileFn = (CompileFn)Marshal.GetDelegateForFunctionPointer(
                    Export(library, "yaal_compile"), typeof(CompileFn));
                freeFn = (FreeFn)Marshal.GetDelegateForFunctionPointer(
                    Export(library, "yaal_free"), typeof(FreeFn));
                return (compileFn, freeFn);
            }
        }

        static IntPtr Load(string path)
        {
            var handle = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? LoadLibrary(path)
                : OpenUnix(path);
            if (handle == IntPtr.Zero)
                throw new CompileException($"failed to load {path}: {LoadError()}");
            return handle;
        }

        static IntPtr Export(IntPtr library, string name)
        {
            var symbol = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? GetProcAddress(library, name)
                : SymbolUnix(library, name);
            if (symbol == IntPtr.Zero)
                throw new CompileException($"yaal shared library is missing {name}");
            return symbol;
        }

        static IntPtr OpenUnix(string path)
        {
            ClearUnixError();
            return RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                ? MacDlopen(path, RtldNow)
                : Dlopen(path, RtldNow);
        }

        static IntPtr SymbolUnix(IntPtr library, string name)
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                ? MacDlsym(library, name)
                : Dlsym(library, name);
        }

        static void ClearUnixError()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                MacDlerror();
            else
                Dlerror();
        }

        static string LoadError()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error()).Message;
            var ptr = RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? MacDlerror() : Dlerror();
            return ptr == IntPtr.Zero ? "dlopen failed" : ReadUtf8(ptr);
        }

        static string LibraryPath()
        {
            var env = Environment.GetEnvironmentVariable("YAAL_LIB");
            if (!string.IsNullOrEmpty(env))
                return env;
            var name = LibraryFileName();
            var rid = RuntimeId();
            foreach (var start in Starts())
            {
                var beside = Path.Combine(start, name);
                if (File.Exists(beside))
                    return beside;
                var packaged = Path.Combine(start, "runtimes", rid, "native", name);
                if (File.Exists(packaged))
                    return packaged;
                var dir = new DirectoryInfo(start);
                while (dir is not null)
                {
                    var runtime = Path.Combine(dir.FullName, "runtimes", rid, "native", name);
                    if (File.Exists(runtime))
                        return runtime;
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

        static IEnumerable<string> Starts()
        {
            yield return AppContext.BaseDirectory;
            var location = typeof(Compiler).GetTypeInfo().Assembly.Location;
            if (!string.IsNullOrEmpty(location))
            {
                var directory = Path.GetDirectoryName(location);
                if (!string.IsNullOrEmpty(directory))
                    yield return directory;
            }
            yield return Directory.GetCurrentDirectory();
        }

        static string LibraryFileName()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return "yaal_ffi.dll";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return "libyaal_ffi.dylib";
            return "libyaal_ffi.so";
        }

        static string RuntimeId()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return "win-x64";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return "osx-arm64";
            return "linux-x64";
        }

        [DllImport("kernel32", CharSet = CharSet.Unicode, SetLastError = true)]
        static extern IntPtr LoadLibrary(string path);

        [DllImport("kernel32", CharSet = CharSet.Ansi, SetLastError = true)]
        static extern IntPtr GetProcAddress(IntPtr module, string name);

        [DllImport("libc.so.6", EntryPoint = "dlopen")]
        static extern IntPtr Dlopen(string file, int mode);

        [DllImport("libc.so.6", EntryPoint = "dlsym")]
        static extern IntPtr Dlsym(IntPtr handle, string name);

        [DllImport("libc.so.6", EntryPoint = "dlerror")]
        static extern IntPtr Dlerror();

        [DllImport("libSystem.B.dylib", EntryPoint = "dlopen")]
        static extern IntPtr MacDlopen(string file, int mode);

        [DllImport("libSystem.B.dylib", EntryPoint = "dlsym")]
        static extern IntPtr MacDlsym(IntPtr handle, string name);

        [DllImport("libSystem.B.dylib", EntryPoint = "dlerror")]
        static extern IntPtr MacDlerror();
    }
}
