// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package yaal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Json {
    private Json() {}

    static String write(Object value) {
        StringBuilder out = new StringBuilder();
        writeInto(out, value);
        return out.toString();
    }

    static Object read(String text) {
        Parser parser = new Parser(text);
        Object value = parser.parseValue();
        parser.skip();
        if (!parser.done()) {
            throw new IllegalArgumentException("trailing JSON");
        }
        return value;
    }

    private static void writeInto(StringBuilder out, Object value) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof String text) {
            out.append('"');
            for (int i = 0; i < text.length(); i++) {
                char ch = text.charAt(i);
                switch (ch) {
                    case '"' -> out.append("\\\"");
                    case '\\' -> out.append("\\\\");
                    case '\n' -> out.append("\\n");
                    case '\r' -> out.append("\\r");
                    case '\t' -> out.append("\\t");
                    default -> {
                        if (ch < 0x20) {
                            out.append(String.format("\\u%04x", (int) ch));
                        } else {
                            out.append(ch);
                        }
                    }
                }
            }
            out.append('"');
        } else if (value instanceof Boolean || value instanceof Integer || value instanceof Long) {
            out.append(value);
        } else if (value instanceof Double || value instanceof Float) {
            double number = ((Number) value).doubleValue();
            if (Double.isNaN(number) || Double.isInfinite(number)) {
                throw new IllegalArgumentException("NaN and infinity are not valid JSON numbers");
            }
            out.append(value);
        } else if (value instanceof Number number) {
            out.append(number);
        } else if (value instanceof Map<?, ?> map) {
            out.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                writeInto(out, String.valueOf(entry.getKey()));
                out.append(':');
                writeInto(out, entry.getValue());
            }
            out.append('}');
        } else if (value instanceof List<?> list) {
            out.append('[');
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    out.append(',');
                }
                writeInto(out, list.get(i));
            }
            out.append(']');
        } else {
            throw new IllegalArgumentException("unsupported JSON value " + value.getClass().getName());
        }
    }

    private static final class Parser {
        private final String text;
        private int index;

        Parser(String text) {
            this.text = text;
        }

        boolean done() {
            return index >= text.length();
        }

        Object parseValue() {
            skip();
            if (done()) {
                throw new IllegalArgumentException("unexpected end of JSON");
            }
            char ch = text.charAt(index);
            if (ch == '{') {
                return parseObject();
            }
            if (ch == '[') {
                return parseArray();
            }
            if (ch == '"') {
                return parseString();
            }
            if (ch == 't' || ch == 'f') {
                return parseBoolean();
            }
            if (ch == 'n') {
                expect("null");
                return null;
            }
            return parseNumber();
        }

        void skip() {
            while (!done() && Character.isWhitespace(text.charAt(index))) {
                index++;
            }
        }

        private Map<String, Object> parseObject() {
            index++;
            Map<String, Object> object = new LinkedHashMap<>();
            skip();
            if (peek('}')) {
                index++;
                return object;
            }
            while (true) {
                skip();
                String key = parseString();
                skip();
                if (!peek(':')) {
                    throw new IllegalArgumentException("expected ':'");
                }
                index++;
                object.put(key, parseValue());
                skip();
                if (peek('}')) {
                    index++;
                    return object;
                }
                if (!peek(',')) {
                    throw new IllegalArgumentException("expected ','");
                }
                index++;
            }
        }

        private List<Object> parseArray() {
            index++;
            List<Object> list = new ArrayList<>();
            skip();
            if (peek(']')) {
                index++;
                return list;
            }
            while (true) {
                list.add(parseValue());
                skip();
                if (peek(']')) {
                    index++;
                    return list;
                }
                if (!peek(',')) {
                    throw new IllegalArgumentException("expected ','");
                }
                index++;
            }
        }

        private String parseString() {
            if (!peek('"')) {
                throw new IllegalArgumentException("expected string");
            }
            index++;
            StringBuilder out = new StringBuilder();
            while (!done()) {
                char ch = text.charAt(index++);
                if (ch == '"') {
                    return out.toString();
                }
                if (ch != '\\') {
                    out.append(ch);
                    continue;
                }
                if (done()) {
                    break;
                }
                char escaped = text.charAt(index++);
                switch (escaped) {
                    case '"', '\\', '/' -> out.append(escaped);
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'u' -> {
                        if (index + 4 > text.length()) {
                            throw new IllegalArgumentException("bad unicode escape");
                        }
                        out.append((char) Integer.parseInt(text.substring(index, index + 4), 16));
                        index += 4;
                    }
                    default -> throw new IllegalArgumentException("bad escape");
                }
            }
            throw new IllegalArgumentException("unclosed string");
        }

        private Boolean parseBoolean() {
            if (text.startsWith("true", index)) {
                index += 4;
                return Boolean.TRUE;
            }
            if (text.startsWith("false", index)) {
                index += 5;
                return Boolean.FALSE;
            }
            throw new IllegalArgumentException("expected boolean");
        }

        private Number parseNumber() {
            int start = index;
            if (peek('-')) {
                index++;
            }
            while (!done() && Character.isDigit(text.charAt(index))) {
                index++;
            }
            boolean fractional = false;
            if (peek('.')) {
                fractional = true;
                index++;
                while (!done() && Character.isDigit(text.charAt(index))) {
                    index++;
                }
            }
            if (!done() && (text.charAt(index) == 'e' || text.charAt(index) == 'E')) {
                fractional = true;
                index++;
                if (peek('+') || peek('-')) {
                    index++;
                }
                while (!done() && Character.isDigit(text.charAt(index))) {
                    index++;
                }
            }
            String token = text.substring(start, index);
            if (token.isEmpty() || token.equals("-")) {
                throw new IllegalArgumentException("expected number");
            }
            if (!fractional) {
                try {
                    return Long.parseLong(token);
                } catch (NumberFormatException ignored) {
                    return Double.parseDouble(token);
                }
            }
            return Double.parseDouble(token);
        }

        private void expect(String literal) {
            if (!text.startsWith(literal, index)) {
                throw new IllegalArgumentException("expected " + literal);
            }
            index += literal.length();
        }

        private boolean peek(char ch) {
            return !done() && text.charAt(index) == ch;
        }
    }
}
