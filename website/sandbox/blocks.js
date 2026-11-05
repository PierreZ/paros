// The operator's blocks (#844 (playground action blocks)). One block is one
// action, written as a sentence with its values inline: a dropdown
// (`<select>`), a number or a name field. A click on the block (not on a
// field) runs it with the values it shows. Each dropdown lists only the
// values that are valid now; a block with a dropdown and no valid value is
// disabled, and its title says why.
//
// A spec is { group, label, blocks: [block] }, and a block is
//   { id, title, parts: [word | field], why?: () => reason | null,
//     run: (values) => void }
// with a field
//   { key, kind: "select", label, options: () => [[value, text]], empty }
//   { key, kind: "number", label, min, max, value }
//   { key, kind: "text", label, auto: () => text }
// The look is the shared `.block` component of website/shared/base.css.

import { el } from "./stage.js";

// Build the groups of blocks into `root`. Returns { refresh }: call it when
// what is valid may have changed (a new topology, a click).
export function mountBlocks(root, groups, { narrow = false } = {}) {
  const mounted = [];
  const sections = groups.map((group, at) => {
    const details = el("details", { className: "block-group" });
    details.dataset.group = group.group;
    details.open = !narrow || at === 0;
    const summary = el("summary", {});
    summary.append(el("span", { className: "swatch" }), el("span", { textContent: group.label }));
    const list = el("div", { className: "blocks" });
    for (const spec of group.blocks) {
      const one = mount(spec);
      mounted.push(one);
      list.append(one.block);
    }
    details.append(summary, list);
    return details;
  });
  root.replaceChildren(...sections);
  const refresh = () => { for (const one of mounted) refreshBlock(one); };
  refresh();
  return { refresh };
}

function mount(spec) {
  const block = el("div", { className: "block", tabIndex: 0 });
  block.setAttribute("role", "button");
  block.dataset.act = spec.id;
  const inputs = {};
  for (const part of spec.parts) {
    if (typeof part === "string") {
      block.append(el("span", { className: "word", textContent: part }));
      continue;
    }
    const input = part.kind === "select" ? el("select", { className: "field" }) : el("input", { className: "field", type: part.kind });
    if (part.kind === "number") {
      input.min = String(part.min);
      input.max = String(part.max);
      input.value = String(part.value);
      input.inputMode = "numeric";
    }
    if (part.kind === "text") {
      input.autocomplete = "off";
      input.spellcheck = false;
      input.addEventListener("input", () => { input.dataset.edited = input.value ? "1" : ""; });
    }
    input.setAttribute("aria-label", part.label);
    input.title = part.label;
    input.dataset.key = part.key;
    inputs[part.key] = input;
    block.append(input);
  }
  const one = { spec, block, inputs };
  const go = () => {
    refreshBlock(one);
    if (block.classList.contains("disabled")) return;
    spec.run(valuesOf(one));
    for (const input of Object.values(inputs)) input.dataset.edited = "";
  };
  block.addEventListener("click", (event) => { if (!event.target.closest(".field")) go(); });
  block.addEventListener("keydown", (event) => {
    if (event.target !== block || (event.key !== "Enter" && event.key !== " ")) return;
    event.preventDefault();
    go();
  });
  return one;
}

// The values a block shows, by field key: numbers clamped to their bounds,
// an empty name replaced by its suggestion.
export function valuesOf({ spec, inputs }) {
  const values = {};
  for (const part of spec.parts) {
    if (typeof part === "string") continue;
    const input = inputs[part.key];
    if (part.kind === "number") values[part.key] = clamp(Number(input.value), part.min, part.max, part.value);
    else if (part.kind === "text") values[part.key] = input.value.trim() || part.auto();
    else values[part.key] = input.value;
  }
  return values;
}

export function clamp(n, min, max, otherwise) {
  if (!Number.isFinite(n)) return otherwise;
  return Math.min(max, Math.max(min, Math.round(n)));
}

function refreshBlock({ spec, block, inputs }) {
  let why = spec.why?.() ?? null;
  for (const part of spec.parts) {
    if (typeof part === "string") continue;
    const input = inputs[part.key];
    if (part.kind === "text") {
      if (!input.dataset.edited) input.value = part.auto();
      continue;
    }
    if (part.kind !== "select") continue;
    const options = part.options().map(([value, text]) => [String(value), text]);
    const shown = options.length ? options : [["", "none"]];
    const same = shown.length === input.options.length
      && shown.every(([value, text], i) => input.options[i].value === value && input.options[i].textContent === text);
    if (!same) {
      const keep = input.value;
      input.replaceChildren(...shown.map(([value, text]) => el("option", { value, textContent: text })));
      if (shown.some(([value]) => value === keep)) input.value = keep;
    }
    if (!options.length) why ??= part.empty;
  }
  block.classList.toggle("disabled", Boolean(why));
  block.setAttribute("aria-disabled", why ? "true" : "false");
  for (const input of Object.values(inputs)) input.disabled = Boolean(why);
  block.title = why ?? spec.title ?? "";
}
