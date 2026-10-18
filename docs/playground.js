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
};

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
  sqlInput.value = preset.sql;
  paramsInput.value = preset.params;
  placeholderInput.value = "question";
  schedule();
}

for (const button of document.querySelectorAll("[data-preset]")) {
  button.addEventListener("click", () => load(button.dataset.preset));
}
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
