# CLI

`yaal` compiles a SQL string into dialect SQL and ordered bind values. Each [GitHub release](https://github.com/kirubasankars/yaal/releases) has a download for Linux x86_64, macOS Apple silicon, and Windows x64.

The tag is `v` plus the version in `crates/yaal-cli/Cargo.toml`. For version `1.1.0` the tag is `v1.1.0`, and the release contains:

- `yaal-1.1.0-linux-x64.tar.gz`, a statically linked `yaal`
- `yaal-1.1.0-macos-arm64.tar.gz`, `yaal` for Apple silicon
- `yaal-1.1.0-windows-x64.zip`, `yaal.exe`
- `yaal-1.1.0-SHA256SUMS.txt`, the sha256 of those three archives

Each archive also contains `LICENSE` and `README.txt`. Replace `1.1.0` below with the version you are installing.

## Linux

```bash
version=1.1.0
curl -fsSL -o "yaal-${version}-linux-x64.tar.gz" \
  "https://github.com/kirubasankars/yaal/releases/download/v${version}/yaal-${version}-linux-x64.tar.gz"
curl -fsSL -o "yaal-${version}-SHA256SUMS.txt" \
  "https://github.com/kirubasankars/yaal/releases/download/v${version}/yaal-${version}-SHA256SUMS.txt"
grep linux-x64 "yaal-${version}-SHA256SUMS.txt" | sha256sum -c -
mkdir -p yaal-cli
tar -xzf "yaal-${version}-linux-x64.tar.gz" -C yaal-cli
sudo install -m 755 yaal-cli/yaal /usr/local/bin/yaal
yaal --version
```

The Linux binary is static, so it does not need a system libc. `yaal --version` prints `yaal 1.1.0`.

## macOS

```bash
version=1.1.0
curl -fsSL -o "yaal-${version}-macos-arm64.tar.gz" \
  "https://github.com/kirubasankars/yaal/releases/download/v${version}/yaal-${version}-macos-arm64.tar.gz"
curl -fsSL -o "yaal-${version}-SHA256SUMS.txt" \
  "https://github.com/kirubasankars/yaal/releases/download/v${version}/yaal-${version}-SHA256SUMS.txt"
grep macos-arm64 "yaal-${version}-SHA256SUMS.txt" | shasum -a 256 -c -
mkdir -p yaal-cli
tar -xzf "yaal-${version}-macos-arm64.tar.gz" -C yaal-cli
sudo install -m 755 yaal-cli/yaal /usr/local/bin/yaal
yaal --version
```

A browser download can be blocked the first time it runs. `xattr -d com.apple.quarantine yaal-cli/yaal` clears that flag.

## Windows

In PowerShell:

```powershell
$version = "1.1.0"
Invoke-WebRequest -Uri "https://github.com/kirubasankars/yaal/releases/download/v$version/yaal-$version-windows-x64.zip" -OutFile "yaal-$version-windows-x64.zip"
Invoke-WebRequest -Uri "https://github.com/kirubasankars/yaal/releases/download/v$version/yaal-$version-SHA256SUMS.txt" -OutFile "yaal-$version-SHA256SUMS.txt"
Get-FileHash "yaal-$version-windows-x64.zip" -Algorithm SHA256
Expand-Archive "yaal-$version-windows-x64.zip" -DestinationPath yaal-cli
.\yaal-cli\yaal.exe --version
```

Compare the printed hash with the `windows-x64` line in the sums file. Windows may ask before an unsigned program runs the first time. Add the `yaal-cli` folder to `PATH` to run `yaal` from any directory.

## Compile

```bash
yaal compile --sql query.sql --params params.json
yaal compile --sql query.sql --param id=1 --placeholder numbered
yaal compile --param id=1 < query.sql
```

SQL comes from `--sql`, or from stdin when `--sql` is omitted and stdin is not a terminal. `yaal compile` with no SQL exits instead of waiting.

`--params` is a JSON object. Repeat `--param name=value` for one argument at a time. A `--param` replaces the same name loaded from `--params`.

A value is `true`, `false`, `null`, an integer, a decimal, or a string. A value that starts with `[`, `{`, or `"` is parsed as JSON, so a list is `--param id=[1, 2]`.

`--placeholder` chooses the bind marker:

- `question` or `?`, the default, writes `?`
- `numbered` or `$` writes `$1`, `$2`, and so on

`--sql--` splits the input into separate statements that share the parameters. Each statement numbers its own `$1`. The SQL syntax is the same as the library. See [optional](optional.md) and [examples](examples.md).

`query.sql`:

```sql
--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
```

`yaal compile --sql query.sql --param id=1` prints:

```json
{
  "statements": [
    {
      "sql": "\nselect *\nfrom users u\nwhere (u.id = ?)\n",
      "parameters": [
        {
          "name": "id",
          "type": "integer",
          "value": 1
        }
      ]
    }
  ]
}
```

The compiled `sql` keeps the newline that followed the header. `active` was omitted, so that `optional(...)` block is gone. An omitted or `null` argument is absent. `0`, `""`, and `false` are present.

The same command with `--placeholder numbered` and `--param id=[1, 2]` on `where optional(u.user_id in ({{id}}...))` compiles that line to `where (u.user_id in ($1, $2))` and two `id` binds, `1` then `2`.

`yaal --help` prints the usage. `yaal --version` prints the version. A compiler error, an unknown argument, or a missing file prints the message on stderr and exits 1. Success exits 0.

## From a checkout

```bash
cargo run -p yaal-cli -- compile --sql query.sql --params params.json
```

That builds the same `yaal` binary and runs `compile`.
