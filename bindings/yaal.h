/* Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
 * Use of this source code is governed by a MIT style
 * license that can be found in the LICENSE file.
 *
 * sql and params_json are UTF-8. params_json is a JSON object, or NULL for {}.
 * placeholder is "question", "numbered", or NULL.
 * The returned string is JSON: {"statements":[...]} or {"error":"..."}.
 * Free it with yaal_free.
 */
#ifndef YAAL_H
#define YAAL_H

char *yaal_compile(const char *sql, const char *params_json, const char *placeholder);
void yaal_free(char *ptr);

#endif
