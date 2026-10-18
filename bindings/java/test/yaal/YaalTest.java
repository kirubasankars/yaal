// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package yaal;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public final class YaalTest {
    private YaalTest() {}

    static final String SQL = """
            --(id, active)--
            select * from users u
            where optional(u.id = {{id}})
              and optional(u.active = {{active}})
            """;

    static final String PARTIAL = """
            --(id, active)--
            select * from users u
            where optional(u.id = {{id}} and u.active = {{active}})
            """;

    public static void main(String[] args) throws Exception {
        Yaal.Compiled compiled = Yaal.compile(SQL, Map.of("id", 1), "question");
        expect(compiled.statements().size() == 1, "one statement");
        Yaal.Statement statement = compiled.statements().get(0);
        expect(norm(statement.sql()).equals("select * from users u where (u.id = ?)"), statement.sql());
        Yaal.Parameter parameter = statement.parameters().get(0);
        expect(parameter.name().equals("id") && parameter.type().equals("integer") && Long.valueOf(1).equals(parameter.value()),
                String.valueOf(parameter));

        Yaal.Compiled expanded = Yaal.compile(
                "--(id)--\nselect * from t where id in ({{id}}...)",
                Map.of("id", List.of(1, 2)),
                "question");
        expect(expanded.statements().get(0).values().equals(List.of(1L, 2L)),
                String.valueOf(expanded.statements().get(0).values()));

        Yaal.Compiled numbered = Yaal.compile(SQL, Map.of("id", 1), "numbered");
        String numberedSql = numbered.statements().get(0).sql();
        expect(numberedSql.contains("$1") && !numberedSql.contains("?"), numberedSql);

        try {
            Yaal.compile(PARTIAL, Map.of("id", 1), "question");
            throw new AssertionError("partial optional compiled");
        } catch (CompileException ex) {
            expect(ex.getMessage().toLowerCase().contains("partial"), ex.getMessage());
        }

        try {
            Yaal.compile("--(id)--\nselect {{id}}\0 or 1 = 1", Map.of("id", 1), "question");
            throw new AssertionError("NUL in sql compiled");
        } catch (CompileException ex) {
            expect(ex.getMessage().contains("NUL"), ex.getMessage());
        }
        fixtureCases();
        System.out.println("ok");
    }

    @SuppressWarnings("unchecked")
    static void fixtureCases() throws Exception {
        Object root = Json.read(Files.readString(casesPath()));
        List<?> cases = (List<?>) root;
        expect(cases.size() > 100, "fixture cases missing");
        for (Object item : cases) {
            Map<String, Object> row = (Map<String, Object>) item;
            String name = String.valueOf(row.get("name"));
            String sql = String.valueOf(row.get("sql"));
            Object params = row.get("params");
            Map<String, ?> values = params instanceof Map<?, ?> map ? (Map<String, ?>) map : Map.of();
            if (row.get("expect_error_contains") instanceof String needle) {
                try {
                    Yaal.compile(sql, values, "question");
                    throw new AssertionError(name + ": expected error " + needle);
                } catch (CompileException ex) {
                    expect(ex.getMessage().toLowerCase().contains(needle.toLowerCase()), name + ": " + ex.getMessage());
                }
                continue;
            }
            Yaal.Compiled compiled = Yaal.compile(sql, values, "question");
            expect(compiled.statements().size() == 1, name);
            Yaal.Statement statement = compiled.statements().get(0);
            expect(norm(statement.sql()).equals(norm(String.valueOf(row.get("expect_sql")))), name + "\n" + statement.sql());
            StringBuilder names = new StringBuilder();
            for (Yaal.Parameter parameter : statement.parameters()) {
                if (!names.isEmpty()) {
                    names.append(',');
                }
                names.append(parameter.name());
            }
            StringBuilder expect = new StringBuilder();
            if (row.get("expect_param_names") instanceof List<?> list) {
                for (Object value : list) {
                    if (!expect.isEmpty()) {
                        expect.append(',');
                    }
                    expect.append(value);
                }
            }
            expect(names.toString().equals(expect.toString()), name + " names " + names + " exp " + expect);
        }
    }

    static Path casesPath() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("tests/fixtures/sql_compile/cases.json");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        throw new AssertionError("tests/fixtures/sql_compile/cases.json not found");
    }

    static String norm(String sql) {
        return String.join(" ", sql.trim().split("\\s+"));
    }

    static void expect(boolean condition, String detail) {
        if (!condition) {
            throw new AssertionError(detail);
        }
    }
}
