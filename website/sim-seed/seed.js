// The seed widget (#307 (paros-sim in wasm)): runs one seed of the paros
// simulation campaign in the browser and shows whether every oracle held.
// The `sim_seed` shortcode mounts it on every `.sim-seed` element. The run is
// `paros_sim::run_chain_seed_view`, compiled to wasm; this file only draws the
// JSON it returns.

const KINDS = [
  { key: "fault", label: "Fault", color: "#c0504d", names: ["sim_fault"] },
  { key: "leader", label: "Leader elected", color: "#c97a2b", names: ["leader_elected"] },
  {
    key: "lost",
    label: "Leadership lost",
    color: "#8a5ac7",
    names: ["leadership_resigned", "leader_quorum_lost"],
  },
  { key: "chosen", label: "Value chosen", color: "#3b8a57", names: ["value_chosen"] },
  {
    key: "cell",
    label: "Cell",
    color: "#3a78c2",
    names: ["cell_initialized", "cell_formed", "machine_serving"],
  },
  {
    key: "other",
    label: "Gap fill, quarantine",
    color: "#8c6d46",
    names: ["election_gap_filled", "journal_quarantined"],
  },
];
const KIND_OF = new Map(KINDS.flatMap((k) => k.names.map((n) => [n, k])));
// Fields that are internal identifiers or hashes: kept in the JSON, not shown.
const HIDDEN_FIELDS = new Set(["vhash", "cell"]);
const MAX_ROWS = 400;
const U64_MAX = (1n << 64n) - 1n;

const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
const pending = [];
worker.onmessage = (message) => pending.shift()?.(message.data);

function runSeed(seed) {
  return new Promise((resolve) => {
    pending.push(resolve);
    worker.postMessage(seed);
  });
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === "class") node.className = value;
    else node.setAttribute(key, value);
  }
  for (const child of children) node.append(child);
  return node;
}

function svg(tag, attrs = {}) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  return node;
}

function validSeed(text) {
  if (!/^\d{1,20}$/.test(text)) return null;
  const value = BigInt(text);
  return value <= U64_MAX ? value.toString() : null;
}

function randomSeed() {
  const words = crypto.getRandomValues(new Uint32Array(2));
  return ((BigInt(words[0]) << 32n) | BigInt(words[1])).toString();
}

function seedFromHash() {
  const match = /(?:^|[#&])seed=(\d+)/.exec(location.hash);
  return match ? validSeed(match[1]) : null;
}

function fieldsText(fields) {
  return Object.entries(fields)
    .filter(([key]) => !HIDDEN_FIELDS.has(key))
    .map(([key, value]) => `${key}=${value}`)
    .join(" ");
}

// The lane of an event: a fault strikes the process its `ip` field names.
function laneOf(event) {
  if (event.source === "sim") return event.fields.ip ?? "sim";
  return event.source;
}

function mount(root) {
  const input = el("input", {
    type: "text",
    inputmode: "numeric",
    "aria-label": "Seed",
    value: seedFromHash() ?? validSeed(root.dataset.seed ?? "1") ?? "1",
  });
  const run = el("button", { class: "ss-run", type: "button" }, "Run the seed");
  const random = el("button", { type: "button" }, "Random seed");
  const verdict = el("div", { class: "ss-verdict", role: "status" });
  const facts = el("div", { class: "ss-facts" });
  const violations = el("ul", { class: "ss-violations" });
  const timeline = el("div", { class: "ss-timeline" });
  const legend = el("div", { class: "ss-legend" });
  const events = el("div", { class: "ss-events" });
  root.replaceChildren(
    el("div", { class: "ss-row" }, el("label", {}, "Seed "), input, run, random),
    verdict,
    facts,
    violations,
    timeline,
    legend,
    events,
    el(
      "p",
      { class: "ss-note" },
      "The network, the disks and the clock are simulated. Times are simulated milliseconds, " +
        "not production numbers. The same seed gives the same run every time, in this page " +
        "and in the native simulation.",
    ),
  );

  const shown = new Set(KINDS.map((k) => k.key));
  let current = null;

  function draw() {
    if (!current) return;
    const visible = current.events.filter((e) => shown.has((KIND_OF.get(e.name) ?? KINDS[5]).key));
    drawTimeline(timeline, current, visible);
    const rows = visible.slice(0, MAX_ROWS).map((e) => {
      const kind = KIND_OF.get(e.name) ?? KINDS[5];
      return el(
        "tr",
        {},
        el("td", {}, `${e.t} ms`),
        el("td", {}, e.source === "sim" ? "sim" : `${e.role} ${e.source}`),
        el(
          "td",
          {},
          el("span", { class: "ss-swatch", style: `background:${kind.color}` }),
          e.name,
        ),
        el("td", {}, fieldsText(e.fields)),
      );
    });
    const table = el(
      "table",
      {},
      el("thead", {}, el("tr", {}, el("th", {}, "Time"), el("th", {}, "Source"), el("th", {}, "Event"), el("th", {}, "Fields"))),
      el("tbody", {}, ...rows),
    );
    const more =
      visible.length > MAX_ROWS
        ? [el("p", { class: "ss-note" }, `The first ${MAX_ROWS} of ${visible.length} events.`)]
        : [];
    events.replaceChildren(table, ...more);
  }

  legend.replaceChildren(
    ...KINDS.map((kind) => {
      const button = el(
        "button",
        { type: "button", "aria-pressed": "true" },
        el("span", { class: "ss-swatch", style: `background:${kind.color}` }),
        kind.label,
      );
      button.addEventListener("click", () => {
        if (shown.has(kind.key)) shown.delete(kind.key);
        else shown.add(kind.key);
        button.classList.toggle("off", !shown.has(kind.key));
        button.setAttribute("aria-pressed", String(shown.has(kind.key)));
        draw();
      });
      return button;
    }),
  );

  async function start() {
    const seed = validSeed(input.value.trim());
    if (seed === null) {
      verdict.className = "ss-verdict bad";
      verdict.textContent = "A seed is a whole number from 0 to 18446744073709551615.";
      return;
    }
    input.value = seed;
    history.replaceState(null, "", `#seed=${seed}`);
    run.disabled = random.disabled = true;
    verdict.className = "ss-verdict";
    verdict.textContent = `Running seed ${seed}…`;
    facts.replaceChildren();
    violations.replaceChildren();
    const { json, wallMs } = await runSeed(seed);
    run.disabled = random.disabled = false;
    const result = JSON.parse(json);
    if (result.error) {
      verdict.className = "ss-verdict bad";
      verdict.textContent = result.error;
      return;
    }
    current = result;
    if (result.green) {
      verdict.className = "ss-verdict ok";
      verdict.textContent = `Seed ${seed}: every oracle held.`;
    } else {
      verdict.className = "ss-verdict bad";
      verdict.textContent = result.failure
        ? `Seed ${seed}: the run failed.`
        : `Seed ${seed}: ${result.violations.length} oracle(s) failed.`;
    }
    violations.replaceChildren(
      ...result.violations.map((v) => el("li", {}, v)),
      ...(result.failure ? [el("li", {}, result.failure)] : []),
    );
    const fact = (label, value) => el("span", {}, `${label} `, el("b", {}, value));
    facts.replaceChildren(
      fact("Simulated time", `${(result.simulated_ms / 1000).toFixed(1)} s`),
      fact("Simulator steps", result.steps.toLocaleString("en")),
      fact("Events shown", result.events.length.toLocaleString("en")),
      fact("Time in this browser", `${(wallMs / 1000).toFixed(1)} s`),
      ...(result.digest ? [fact("Digest", result.digest)] : []),
    );
    draw();
  }

  run.addEventListener("click", start);
  random.addEventListener("click", () => {
    input.value = randomSeed();
    start();
  });
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") start();
  });
}

function drawTimeline(container, run, visible) {
  const order = ["client", "machine", "node", "matchmaker", "proxy", "replica", "joiner", "frontend", "sim", "other"];
  const roleOf = new Map();
  for (const e of run.events) {
    if (e.source !== "sim") roleOf.set(e.source, e.role);
  }
  const lanes = [...new Set(run.events.map(laneOf))].sort((a, b) => {
    const ra = order.indexOf(roleOf.get(a) ?? "sim");
    const rb = order.indexOf(roleOf.get(b) ?? "sim");
    return ra - rb || a.localeCompare(b, "en", { numeric: true });
  });
  const laneHeight = 18;
  const left = 150;
  const width = 900;
  const top = 8;
  const height = top + lanes.length * laneHeight + 24;
  const end = Math.max(run.simulated_ms, 1);
  const x = (t) => left + ((width - left - 10) * t) / end;
  const root = svg("svg", {
    viewBox: `0 0 ${width} ${height}`,
    role: "img",
    "aria-label": `Timeline of ${visible.length} events over ${lanes.length} processes`,
  });
  lanes.forEach((lane, i) => {
    const y = top + i * laneHeight + laneHeight / 2;
    root.append(
      svg("line", { x1: left, x2: width - 10, y1: y, y2: y, stroke: "currentColor", "stroke-opacity": "0.15" }),
    );
    const label = svg("text", { x: left - 6, y: y + 4, "text-anchor": "end" });
    label.textContent = lane === "sim" ? "sim" : `${roleOf.get(lane) ?? "sim"} ${lane}`;
    root.append(label);
  });
  const laneIndex = new Map(lanes.map((lane, i) => [lane, i]));
  for (const e of visible) {
    const kind = KIND_OF.get(e.name) ?? KINDS[5];
    const y = top + laneIndex.get(laneOf(e)) * laneHeight + laneHeight / 2;
    const dot = svg("circle", { cx: x(e.t), cy: y, r: kind.key === "chosen" ? 2 : 3.5, fill: kind.color });
    const title = svg("title");
    title.textContent = `${e.t} ms ${e.name} ${fieldsText(e.fields)}`;
    dot.append(title);
    root.append(dot);
  }
  const axisY = top + lanes.length * laneHeight + 14;
  for (let i = 0; i <= 4; i++) {
    const t = (end * i) / 4;
    const tick = svg("text", { x: x(t), y: axisY, "text-anchor": i === 0 ? "start" : i === 4 ? "end" : "middle" });
    tick.textContent = `${(t / 1000).toFixed(1)} s`;
    root.append(tick);
  }
  container.replaceChildren(root);
}

for (const root of document.querySelectorAll(".sim-seed")) mount(root);
