import init, { compile } from "./yaal_wasm.js";

const presets = {
  optional: {
    sql: `--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})`,
    params: `{
  "id": 1
}`,
  },
  several: {
    sql: `--(a, b)--
select *
from t
where optional(
  col1 = {{a}}
  and col3 = {{b}}
)`,
    params: `{
  "a": 1,
  "b": 1
}`,
  },
  list: {
    sql: `--(cv)--
select *
from t
where optional(col in ({{cv}}...))`,
    params: `{
  "cv": [1, 2, 3]
}`,
  },
  when: {
    sql: `--(apply, id)--
select * from t where optional_when({{apply}}, id = {{id}})`,
    params: `{
  "apply": false,
  "id": 4
}`,
  },
  groups: {
    sql: `--(pairs blob, active)--
select * from users u
where optional_groups_or({{pairs}}, u.id = {{self.id}} and u.active = {{active}})`,
    params: `{
  "pairs": [{"id": 1}, {"id": 2}],
  "active": 1
}`,
  },
  groupsAnd: {
    sql: `--(pairs blob)--
select *
from t
where optional_groups_and(
  {{pairs}},
  id = {{self.id}}
)`,
    params: `{
  "pairs": [
    {"id": 1},
    {"id": 2}
  ]
}`,
  },
  groupList: {
    sql: `--(pairs blob)--
select u.user_id
from users u
where optional_groups_or(
  {{pairs}},
  u.user_id = {{self.id}}
  and u.role_id in ({{self.ids}}...)
)`,
    params: `{
  "pairs": [
    {"id": 1, "ids": [8, 9]}
  ]
}`,
  },
  sort: {
    sql: `--(sort, dir)--
select *
from users u
order by
  sort({{sort}}, name = u.user_name, id = u.user_id)
  dir({{dir}}),
  u.user_id asc`,
    params: `{
  "sort": "name,id",
  "dir": "desc,asc"
}`,
  },
  filterSort: {
    sql: `--(active, sort, dir)--
select u.user_id, u.user_name
from users u
where 1 = 1
  and optional(u.active = {{active}})
order by
  sort({{sort}}, name = u.user_name, id = u.user_id)
  dir({{dir}})`,
    params: `{
  "active": 1,
  "sort": "name",
  "dir": "desc"
}`,
  },
  whenGroups: {
    sql: `--(apply, pairs blob)--
select *
from t
where z = 1
  and optional_when(
    {{apply}},
    optional_groups_or({{pairs}}, id = {{self.id}})
  )`,
    params: `{
  "apply": true,
  "pairs": [
    {"id": 1},
    {"id": 2}
  ]
}`,
  },
  whenAndGroups: {
    sql: `--(apply, pairs blob)--
select *
from t
where z = 1
  and optional_when(
    {{apply}},
    optional_groups_and({{pairs}}, id = {{self.id}})
  )`,
    params: `{
  "apply": true,
  "pairs": [
    {"id": 1},
    {"id": 2}
  ]
}`,
  },
  whenInGroup: {
    sql: `--(pairs blob, flag)--
select *
from t
where optional_groups_or(
  {{pairs}},
  c1 = {{self.cv}}
  and optional_when({{flag}}, c2 = {{self.cv2}})
)`,
    params: `{
  "flag": true,
  "pairs": [
    {"cv": 1, "cv2": 1},
    {"cv": 1, "cv2": 2}
  ]
}`,
  },
  optionalGroup: {
    sql: `--(pairs blob, x)--
select *
from t
where z = 1
  and optional(
    a = {{x}}
    and optional_groups_or({{pairs}}, id = {{self.id}})
  )`,
    params: `{
  "x": 1,
  "pairs": [
    {"id": 1},
    {"id": 2}
  ]
}`,
  },
  optionalInGroup: {
    sql: `--(pairs blob, flag)--
select *
from t
where optional_groups_or(
  {{pairs}},
  col1 = {{self.cv1}}
  and optional(col2 = {{flag}})
)`,
    params: `{
  "flag": 1,
  "pairs": [
    {"cv1": 1},
    {"cv1": 2}
  ]
}`,
  },
  whenOptionalGroups: {
    sql: `--(apply, active, pairs blob)--
select *
from users u
where 1 = 1
  and optional_when(
    {{apply}},
    optional(u.active = {{active}})
    and optional_groups_or({{pairs}}, u.id = {{self.id}})
  )`,
    params: `{
  "apply": true,
  "active": 1,
  "pairs": [
    {"id": 1},
    {"id": 2}
  ]
}`,
  },
};

const exampleInput = document.querySelector("#example");
const sqlInput = document.querySelector("#sql");
const paramsInput = document.querySelector("#params");
const placeholderInput = document.querySelector("#placeholder");
const result = document.querySelector("#result");
const resultCode = result.querySelector("code");

function show(text, error) {
  resultCode.textContent = text;
  result.classList.toggle("error", error);
}

function format(value) {
  if (value.error) {
    return { text: value.error, error: true };
  }
  const chunks = value.statements.map((statement) => {
    const lines = [statement.sql.trim()];
    if (statement.parameters.length) {
      lines.push("");
      for (const param of statement.parameters) {
        lines.push(`${param.name}  ${param.type}  ${JSON.stringify(param.value)}`);
      }
    }
    return lines.join("\n");
  });
  return { text: chunks.join("\n\n"), error: false };
}

let timer = 0;
function schedule() {
  window.clearTimeout(timer);
  timer = window.setTimeout(run, 120);
}

function run() {
  let text;
  try {
    text = compile(sqlInput.value, paramsInput.value, placeholderInput.value);
  } catch (err) {
    show(String(err), true);
    return;
  }
  let value;
  try {
    value = JSON.parse(text);
  } catch (err) {
    show(text, true);
    return;
  }
  const formatted = format(value);
  show(formatted.text, formatted.error);
}

function load(name) {
  const preset = presets[name];
  exampleInput.value = name;
  sqlInput.value = preset.sql;
  paramsInput.value = preset.params;
  placeholderInput.value = "question";
  schedule();
}

exampleInput.addEventListener("change", () => load(exampleInput.value));
sqlInput.addEventListener("input", schedule);
paramsInput.addEventListener("input", schedule);
placeholderInput.addEventListener("change", schedule);

const ready = init().then(
  () => {
    load("optional");
  },
  () => {
    show("The compiler did not load.", true);
  }
);
void ready;
