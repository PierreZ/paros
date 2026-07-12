// shell.js — the one instrument chrome shared by every mode.
//
// The shell owns everything that is NOT mode-specific: the flavour tabs, the
// transport (prev / next / play-pause / step counter / speed / seed / replay), the
// progress area (phase track or leader/committed indicator), the narration card,
// the digest chips, the oracle badge, the responsive SVG stage (viewBox +
// ResizeObserver), the keyboard map, the node inspector, and the single glowing
// particle animation. A *mode* is a renderer object (see registry.js); adding one
// never touches this file — that is the whole point.
//
// Because a RunResult is mode-agnostic (the mode is a presentation choice over one
// payload), switching flavour just swaps the renderer and re-derives frames from
// the same run — no wasm re-run, no reload.

import { C, PHASE, PHASE_TRACK, PHASE_LABEL, RADIUS, FONT_SANS, FONT_MONO } from './tokens.js';
import { el, clear, defs } from './svg.js';
import { oracleBadge } from './adapter.js';

const STEP_MS = 1100; // particle travel time for one teaching step at 1×

let styleInjected = false;
function injectStyle() {
  if (styleInjected) return;
  styleInjected = true;
  const css = `
  .paros { --canvas:${C.canvas}; --panel:${C.panel}; --stage:${C.stage}; --inset:${C.inset};
    --hair:${C.hairline}; --soft:${C.softline}; --text:${C.text}; --muted:${C.muted};
    --faint:${C.faint}; --dim:${C.dim};
    box-sizing:border-box; width:100%; max-width:900px; margin:0 auto; color:var(--text);
    font-family:${FONT_SANS}; background:var(--panel); border:1px solid var(--hair);
    border-radius:${RADIUS.panel}px; padding:20px; display:flex; flex-direction:column; gap:16px; }
  .paros *,.paros *::before,.paros *::after{ box-sizing:border-box; }
  .paros .mono{ font-family:${FONT_MONO}; }
  .paros button{ font-family:inherit; color:var(--text); background:none; border:none;
    cursor:pointer; padding:0; }
  .paros button:focus-visible,.paros [tabindex]:focus-visible{ outline:2px solid ${PHASE.prepare};
    outline-offset:2px; border-radius:6px; }

  .paros-appbar{ display:flex; align-items:flex-end; justify-content:space-between; gap:16px;
    border-bottom:1px solid var(--hair); padding-bottom:12px; }
  .paros-brand{ font-size:15px; font-weight:700; letter-spacing:.02em; }
  .paros-tabs{ display:flex; gap:20px; align-items:flex-end; }
  .paros-tab{ position:relative; font-size:13px; color:var(--muted); padding-bottom:6px;
    letter-spacing:.01em; }
  .paros-tab .cap{ display:block; font-size:9px; color:${PHASE.prepare}; letter-spacing:.08em;
    text-transform:uppercase; margin-top:3px; min-height:.9em; text-align:center; }
  .paros-tab[aria-selected=true]{ color:var(--text); }
  .paros-tab[aria-selected=true]::after{ content:''; position:absolute; left:0; right:0; bottom:0;
    height:2px; background:${PHASE.prepare}; border-radius:2px; }

  .paros-stagewrap{ background:var(--stage); border:1px solid var(--hair);
    border-radius:${RADIUS.stage}px; padding:8px; position:relative; overflow:hidden; }
  .paros-stage{ display:block; width:100%; height:auto; }
  .paros-swipe{ position:absolute; top:0; right:0; bottom:0; width:44px; pointer-events:none;
    display:none; align-items:center; justify-content:flex-end; padding-right:6px;
    background:linear-gradient(90deg,transparent,var(--stage)); font-size:9px; color:var(--faint);
    writing-mode:vertical-rl; text-orientation:mixed; letter-spacing:.1em; }
  .paros.is-mobile .paros-stagewrap{ overflow-x:auto; }
  .paros.is-mobile .paros-stagewrap.can-swipe .paros-swipe{ display:flex; }

  .paros-progress{ min-height:8px; }
  .paros-track{ width:100%; height:auto; display:block; }
  .paros-multiprog{ display:flex; align-items:center; gap:12px; font-size:12px; color:var(--muted);
    font-family:${FONT_MONO}; flex-wrap:wrap; }
  .paros-meter{ flex:1 1 120px; height:6px; background:var(--inset); border-radius:3px;
    overflow:hidden; min-width:100px; }
  .paros-meter i{ display:block; height:100%; background:${PHASE.chosen}; border-radius:3px; }

  .paros-card{ background:var(--inset); border:1px solid var(--hair); border-radius:${RADIUS.card}px;
    padding:14px 16px 14px 18px; position:relative; overflow:hidden; }
  .paros-card::before{ content:''; position:absolute; left:0; top:0; bottom:0; width:4px;
    background:var(--accent,${C.neutralRing}); }
  .paros-card h3{ margin:0 0 6px; font-size:18px; font-weight:700; color:var(--accent,var(--text)); }
  .paros-card .l1{ font-size:14px; color:#C9D1DC; line-height:1.5; }
  .paros-card .l2{ font-size:13px; color:var(--muted); line-height:1.5; margin-top:2px; }
  .paros-card .cap{ position:absolute; top:12px; right:14px; font-family:${FONT_MONO};
    font-size:10px; color:var(--faint); letter-spacing:.03em; }

  .paros-digest{ display:flex; flex-wrap:wrap; gap:8px; }
  .paros-digest:empty{ display:none; }
  .paros-chip{ background:var(--inset); border:1px solid var(--hair); border-radius:9px;
    padding:6px 10px; font-family:${FONT_MONO}; }
  .paros-chip b{ display:block; font-size:13px; font-weight:700; color:var(--text); }
  .paros-chip span{ font-size:9px; color:var(--muted); text-transform:uppercase; letter-spacing:.05em; }

  .paros-oracle{ display:flex; flex-wrap:wrap; gap:14px; font-size:11px; color:var(--muted);
    font-family:${FONT_MONO}; }
  .paros-oracle span{ display:inline-flex; align-items:center; gap:6px; }
  .paros-oracle i{ width:7px; height:7px; border-radius:50%; display:inline-block; }

  .paros-transport{ display:flex; align-items:center; gap:14px; border-top:1px solid var(--hair);
    padding-top:14px; flex-wrap:wrap; }
  .paros-nav{ display:flex; align-items:center; gap:12px; }
  .paros-tri{ color:var(--muted); font-size:16px; line-height:1; padding:4px; }
  .paros-tri:hover{ color:var(--text); }
  .paros-play{ width:30px; height:30px; border-radius:50%; background:#1B2230; border:1px solid #2C3546;
    display:inline-flex; align-items:center; justify-content:center; color:var(--text); font-size:13px; }
  .paros-count{ font-family:${FONT_MONO}; font-size:12px; color:var(--muted); }
  .paros-right{ margin-left:auto; display:flex; align-items:center; gap:14px; font-family:${FONT_MONO};
    font-size:11px; color:var(--faint); }
  .paros-right input[type=number]{ width:70px; background:var(--inset); color:var(--text);
    border:1px solid var(--hair); border-radius:7px; padding:5px 8px; font-family:${FONT_MONO};
    font-size:12px; }
  .paros-right input[type=range]{ accent-color:${PHASE.prepare}; width:74px; }
  .paros-right button{ color:var(--faint); }
  .paros-right button:hover{ color:var(--text); }

  .paros-inspector{ display:none; background:var(--inset); border:1px solid var(--hair);
    border-radius:${RADIUS.card}px; padding:12px 14px; }
  .paros-inspector.show{ display:block; }
  .paros-inspector .ihead{ display:flex; justify-content:space-between; align-items:center;
    margin-bottom:8px; }
  .paros-inspector .ititle{ font-family:${FONT_MONO}; font-size:12px; color:var(--text); }
  .paros-inspector .iclose{ color:var(--muted); font-size:14px; }
  .paros-inspector .isec{ font-size:9px; text-transform:uppercase; letter-spacing:.06em;
    color:var(--faint); margin:8px 0 4px; }
  .paros-inspector .irow{ display:flex; justify-content:space-between; gap:16px; font-family:${FONT_MONO};
    font-size:12px; padding:2px 0; }
  .paros-inspector .irow .k{ color:var(--muted); }
  .paros-inspector .irow .v{ color:var(--text); text-align:right; display:inline-flex; align-items:center;
    gap:6px; }
  .paros-inspector .sw{ width:10px; height:10px; border-radius:3px; display:inline-block; }

  @media (max-width:640px){
    .paros{ padding:14px; border-radius:16px; }
    .paros-appbar{ flex-direction:column; align-items:stretch; gap:8px; }
    /* the caption is absolutely positioned top-right on desktop; on a narrow card a
       long heading runs underneath it, so let it flow after the text instead */
    .paros-card .cap{ position:static; display:block; margin-top:8px; text-align:right; }
    .paros-inspector{ position:fixed; left:0; right:0; bottom:0; z-index:50; border-radius:14px 14px 0 0;
      box-shadow:0 -8px 24px rgba(0,0,0,.5); }
  }
  @media (prefers-reduced-motion:reduce){ .paros *{ transition:none !important; } }
  `;
  const s = document.createElement('style');
  s.textContent = css;
  document.head.appendChild(s);
}

export class Shell {
  constructor(root, { registry, modeId, run, seed, params, onModeChange }) {
    injectStyle();
    this.root = root;
    this.registry = registry;
    this.modeId = modeId;
    this.renderer = registry[modeId];
    this.run = run;
    this.seed = seed;
    this.params = params || new URLSearchParams();
    this.onModeChange = onModeChange || (() => {});
    this.reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    this.frames = [];
    this.idx = 0;
    this.playing = false;
    this.speed = 1;
    this.selection = null;
    this.tick = null;      // current frame's particle updater
    this.rafId = 0;
    this.animStart = 0;
    this.mobile = false;
    this._build();
  }

  // ---- DOM scaffold --------------------------------------------------------
  _build() {
    const r = this.root;
    r.className = 'paros';
    clear(r);

    // app bar
    const tabs = document.createElement('div');
    tabs.className = 'paros-tabs';
    this.tabEls = {};
    for (const id of Object.keys(this.registry)) {
      const rr = this.registry[id];
      if (!rr) continue;
      const b = document.createElement('button');
      b.className = 'paros-tab';
      b.dataset.mode = id;
      b.setAttribute('role', 'tab');
      b.innerHTML = `${rr.label}<span class="cap"></span>`;
      b.addEventListener('click', () => this.switchMode(id));
      tabs.appendChild(b);
      this.tabEls[id] = b;
    }
    const appbar = document.createElement('div');
    appbar.className = 'paros-appbar';
    const brand = document.createElement('div');
    brand.className = 'paros-brand';
    brand.textContent = 'paros';
    appbar.appendChild(brand);
    appbar.appendChild(tabs);
    r.appendChild(appbar);

    // stage
    const wrap = document.createElement('div');
    wrap.className = 'paros-stagewrap';
    this.svg = el('svg', { class: 'paros-stage', xmlns: 'http://www.w3.org/2000/svg' });
    this.svg.appendChild(defs());
    this.stageG = el('g', { class: 'paros-stage-g' });
    this.svg.appendChild(this.stageG);
    wrap.appendChild(this.svg);
    const swipe = document.createElement('div');
    swipe.className = 'paros-swipe';
    swipe.textContent = 'swipe roles ›';
    wrap.appendChild(swipe);
    r.appendChild(wrap);

    // progress area
    this.progressEl = document.createElement('div');
    this.progressEl.className = 'paros-progress';
    r.appendChild(this.progressEl);

    // narration card
    this.cardEl = document.createElement('div');
    this.cardEl.className = 'paros-card';
    r.appendChild(this.cardEl);

    // digest chips
    this.digestEl = document.createElement('div');
    this.digestEl.className = 'paros-digest';
    r.appendChild(this.digestEl);

    // oracle badge
    this.oracleEl = document.createElement('div');
    this.oracleEl.className = 'paros-oracle';
    r.appendChild(this.oracleEl);

    // transport
    this._buildTransport();
    r.appendChild(this.transportEl);

    // inspector
    this.inspectorEl = document.createElement('div');
    this.inspectorEl.className = 'paros-inspector';
    r.appendChild(this.inspectorEl);

    // responsive: watch our own width
    this.ro = new ResizeObserver(() => this._onResize());
    this.ro.observe(r);

    // keyboard (scoped to document but ignores when typing in the seed field)
    this._keyHandler = (e) => this._onKey(e);
    document.addEventListener('keydown', this._keyHandler);

    if (this.run) this.loadRun(this.run, this.seed);
  }

  _buildTransport() {
    const t = document.createElement('div');
    t.className = 'paros-transport';
    const nav = document.createElement('div');
    nav.className = 'paros-nav';
    this.prevBtn = this._btn('‹', 'paros-tri', () => this.step(-1), 'previous step');
    this.playBtn = this._btn('▶', 'paros-play', () => this.togglePlay(), 'play / pause');
    this.nextBtn = this._btn('›', 'paros-tri', () => this.step(1), 'next step');
    nav.append(this.prevBtn, this.playBtn, this.nextBtn);
    this.countEl = document.createElement('span');
    this.countEl.className = 'paros-count';
    const right = document.createElement('div');
    right.className = 'paros-right';
    // speed
    this.speedSel = document.createElement('select');
    this.speedSel.style.cssText = 'background:var(--inset);color:var(--text);border:1px solid var(--hair);border-radius:6px;font-family:inherit;font-size:11px;padding:2px 4px;';
    for (const s of [0.5, 1, 2, 3]) {
      const o = document.createElement('option');
      o.value = String(s); o.textContent = s + '×';
      if (s === 1) o.selected = true;
      this.speedSel.appendChild(o);
    }
    this.speedSel.setAttribute('aria-label', 'speed');
    this.speedSel.addEventListener('change', () => { this.speed = parseFloat(this.speedSel.value); });
    // seed
    const seedWrap = document.createElement('label');
    seedWrap.style.cssText = 'display:inline-flex;gap:6px;align-items:center;';
    seedWrap.append('seed');
    this.seedInput = document.createElement('input');
    this.seedInput.type = 'number';
    this.seedInput.min = '0';
    this.seedInput.setAttribute('aria-label', 'seed');
    this.seedInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') this._runSeed(); });
    seedWrap.appendChild(this.seedInput);
    // replay
    this.replayBtn = this._btn('↻ replay', '', () => this.replay(), 'replay this run');
    right.append(this.speedSel, seedWrap, this.replayBtn);
    t.append(nav, this.countEl, right);
    this.transportEl = t;
  }

  _btn(text, cls, on, label) {
    const b = document.createElement('button');
    if (cls) b.className = cls;
    b.textContent = text;
    if (label) b.setAttribute('aria-label', label);
    b.addEventListener('click', on);
    return b;
  }

  // ---- run + frame lifecycle ----------------------------------------------
  loadRun(run, seed) {
    this.run = run;
    if (seed !== undefined) this.seed = seed;
    this.seedInput.value = String(this.seed);
    this.frames = this.renderer.frames(run);
    this.idx = 0;
    this.selection = null;
    this._syncTabs();
    this._renderDigest();
    this._renderOracle();
    this._layout(); // sets viewBox for current breakpoint
    const still = this.params.has('still') ? parseInt(this.params.get('still'), 10) : NaN;
    if (Number.isFinite(still)) {
      this.showFrame(this._frameForLeg(still), 1);
      this._setTransportEnabled(false);
      return;
    }
    if (this.renderer.transport === 'still') {
      this.showFrame(0, 1);
      this._setTransportEnabled(false);
      return;
    }
    this._setTransportEnabled(true);
    // Do NOT auto-advance on load (book rule: the default view is a still frame,
    // not a running clock). Show frame 0 with one gentle particle travel, then
    // rest; the reader drives play/step from there.
    this.showFrame(0, this.reduced ? 1 : 0);
  }

  // Map an old-style ?still=<protocol leg index> to a frame index (nearest frame
  // whose time is ≤ that leg's departure). Preserves the screenshot contract.
  _frameForLeg(k) {
    const p = this.run.protocol;
    if (!p.length || !this.frames.length) return 0;
    k = Math.max(0, Math.min(k, p.length - 1));
    const t = p[k].depart_ms;
    let best = 0;
    for (let i = 0; i < this.frames.length; i++) if (this.frames[i].timeMs <= t) best = i;
    return best;
  }

  _setTransportEnabled(on) {
    this.transportEl.querySelector('.paros-nav').style.visibility = on ? '' : 'hidden';
    this.countEl.style.visibility = on ? '' : 'hidden';
    this.speedSel.style.display = on ? '' : 'none';
  }

  showFrame(i, startT) {
    if (!this.frames.length) return;
    this.idx = Math.max(0, Math.min(i, this.frames.length - 1));
    const frame = this.frames[this.idx];
    this._draw(frame);
    this._renderNarration(frame);
    this._renderProgress(frame);
    this._renderCount();
    if (this.selection !== null) this._renderInspector(frame);
    // particle animation
    this._stopAnim();
    const t0 = startT === undefined ? 0 : startT;
    if (this.reduced || this.renderer.transport === 'still') {
      if (this.tick) this.tick(1);
    } else {
      this._animateFrom(t0);
    }
  }

  _draw(frame) {
    clear(this.stageG);
    // re-add the blur filter def (cleared with the group's siblings? no — defs is a
    // sibling of stageG, not cleared). stageG only holds frame content.
    const ctx = {
      stage: this.stageG,
      dims: { w: this._vw, h: this._vh, mobile: this.mobile },
      run: this.run,
      selection: this.selection,
      reduced: this.reduced,
      onSelect: (id) => this.select(id),
    };
    const res = this.renderer.render(frame, ctx) || {};
    this.tick = typeof res.tick === 'function' ? res.tick : null;
  }

  // Particle travel 0→1 over one step, then (if playing) advance to the next frame.
  _animateFrom(t0) {
    const dur = STEP_MS / this.speed;
    this.animStart = null;
    const loop = (ts) => {
      if (this.animStart === null) this.animStart = ts - t0 * dur;
      const t = Math.min(1, (ts - this.animStart) / dur);
      if (this.tick) this.tick(t);
      if (t < 1) { this.rafId = requestAnimationFrame(loop); return; }
      this.rafId = 0;
      if (this.playing) {
        if (this.idx < this.frames.length - 1) this.showFrame(this.idx + 1, 0);
        else { this.playing = false; this._renderPlay(); }
      }
    };
    this.rafId = requestAnimationFrame(loop);
  }

  _stopAnim() { if (this.rafId) { cancelAnimationFrame(this.rafId); this.rafId = 0; } }

  // ---- transport actions ---------------------------------------------------
  play() { if (this.frames.length <= 1) return; this.playing = true; this._renderPlay();
    if (this.idx >= this.frames.length - 1) this.showFrame(0, 0); else this._animateFrom(0); }
  pause() { this.playing = false; this._stopAnim(); this._renderPlay(); if (this.tick) this.tick(1); }
  togglePlay() { this.playing ? this.pause() : this.play(); }
  step(dir) { this.playing = false; this._renderPlay(); this.showFrame(this.idx + dir, this.reduced ? 1 : 0); }
  replay() { this.selection = null; this._hideInspector();
    if (this.renderer.transport === 'still') { this.showFrame(0, 1); return; }
    this.showFrame(0, 0); this.play(); }

  _renderPlay() { this.playBtn.textContent = this.playing ? '❚❚' : '▶'; }
  _renderCount() { this.countEl.textContent = `step ${this.idx + 1} / ${this.frames.length}`; }

  // ---- mode switch (in-place; no wasm re-run) ------------------------------
  switchMode(id) {
    if (id === this.modeId || !this.registry[id]) return;
    this.modeId = id;
    this.renderer = this.registry[id];
    this.selection = null;
    this._hideInspector();
    // keep the URL shareable
    const u = new URL(location.href);
    if (id === 'single') u.searchParams.delete('mode'); else u.searchParams.set('mode', id);
    history.replaceState(null, '', u);
    this.onModeChange(id);
    this.loadRun(this.run, this.seed);
  }

  _syncTabs() {
    for (const [id, b] of Object.entries(this.tabEls)) {
      const on = id === this.modeId;
      b.setAttribute('aria-selected', on ? 'true' : 'false');
      b.querySelector('.cap').textContent = (id === 'single' && on) ? 'start here' : '';
    }
  }

  // ---- selection / inspector ----------------------------------------------
  select(id) {
    this.selection = this.selection === id ? null : id;
    if (this.selection === null) { this._hideInspector(); this._draw(this.frames[this.idx]); if (this.tick) this.tick(1); return; }
    this._draw(this.frames[this.idx]);
    if (this.tick) this.tick(1);
    this._renderInspector(this.frames[this.idx]);
  }
  _hideInspector() { this.inspectorEl.classList.remove('show'); }

  _renderInspector(frame) {
    const rows = this.renderer.inspect(frame, this.selection) || [];
    if (!rows.length) { this._hideInspector(); return; }
    clear(this.inspectorEl);
    const head = document.createElement('div');
    head.className = 'ihead';
    const title = document.createElement('div');
    title.className = 'ititle';
    title.textContent = rows.title || `node · N${this.selection}`;
    const close = this._btn('✕', 'iclose', () => { this.selection = null; this._hideInspector(); this._draw(frame); if (this.tick) this.tick(1); }, 'close inspector');
    head.append(title, close);
    this.inspectorEl.appendChild(head);
    let curGroup = null;
    for (const row of rows) {
      if (row.group && row.group !== curGroup) {
        curGroup = row.group;
        const s = document.createElement('div'); s.className = 'isec'; s.textContent = row.group;
        this.inspectorEl.appendChild(s);
      }
      const r = document.createElement('div'); r.className = 'irow';
      const k = document.createElement('span'); k.className = 'k'; k.textContent = row.k;
      const v = document.createElement('span'); v.className = 'v';
      if (row.swatch) { const sw = document.createElement('span'); sw.className = 'sw'; sw.style.background = row.swatch; v.appendChild(sw); }
      v.append(document.createTextNode(row.v));
      r.append(k, v);
      this.inspectorEl.appendChild(r);
    }
    this.inspectorEl.classList.add('show');
  }

  // ---- narration / progress / digest / oracle ------------------------------
  _renderNarration(frame) {
    const n = this.renderer.narrate(frame) || { phase: 'idle', lines: [] };
    const color = n.color || PHASE[n.phase] || C.neutralRing;
    this.cardEl.style.setProperty('--accent', color);
    clear(this.cardEl);
    const h = document.createElement('h3');
    h.textContent = n.title || PHASE_LABEL[n.phase] || n.phase || '';
    this.cardEl.appendChild(h);
    (n.lines || []).forEach((line, i) => {
      const d = document.createElement('div');
      d.className = i === 0 ? 'l1' : 'l2';
      d.textContent = line;
      this.cardEl.appendChild(d);
    });
    const cap = document.createElement('div');
    cap.className = 'cap';
    cap.textContent = `${this.renderer.seedCaption ? this.renderer.seedCaption(this.seed) : 'lesson'} · seed ${this.seed}`;
    this.cardEl.appendChild(cap);
  }

  _renderProgress(frame) {
    const p = this.renderer.progress ? this.renderer.progress(this.run, frame) : { type: 'none' };
    clear(this.progressEl);
    if (!p || p.type === 'none') return;
    if (p.type === 'track') this.progressEl.appendChild(this._phaseTrack(p));
    else if (p.type === 'committed') this.progressEl.appendChild(this._committedProg(p));
  }

  // The single-decree 5-stop phase track (doubles as legend), grouped Phase 1 / 2.
  // The svg scales to the shell width, so on mobile the viewBox is narrower and the
  // type larger — with the desktop 1000-unit box a 360px shell renders ~4px labels.
  _phaseTrack(p) {
    const m = this.mobile;
    const W = m ? 560 : 1000, H = m ? 132 : 96;
    const svg = el('svg', { class: 'paros-track', viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: 'xMidYMid meet' });
    const n = PHASE_TRACK.length;
    const padX = m ? 54 : 90, y = m ? 42 : 30;
    const fNum = m ? 15 : 11, fLabel = m ? 16 : 12, fBracket = m ? 14 : 10;
    const xOf = (i) => padX + (W - 2 * padX) * (i / (n - 1));
    const activeIdx = p.activeIndex;
    // baseline up to active in phase colour, rest hairline
    const activeColor = PHASE[PHASE_TRACK[Math.max(0, activeIdx)].phase];
    svg.appendChild(el('line', { x1: padX, y1: y, x2: xOf(activeIdx < 0 ? 0 : activeIdx), y2: y, stroke: activeColor, 'stroke-width': 2 }));
    svg.appendChild(el('line', { x1: xOf(activeIdx < 0 ? 0 : activeIdx), y1: y, x2: W - padX, y2: y, stroke: C.hairline, 'stroke-width': 2 }));
    PHASE_TRACK.forEach((stop, i) => {
      const x = xOf(i);
      const on = i === activeIdx;
      const done = i < activeIdx;
      const col = PHASE[stop.phase];
      if (on) {
        svg.appendChild(el('circle', { cx: x, cy: y, r: m ? 14 : 12, fill: 'none', stroke: col, 'stroke-width': 1.5, opacity: 0.5 }));
        svg.appendChild(el('circle', { cx: x, cy: y, r: m ? 8 : 7, fill: col }));
      } else {
        svg.appendChild(el('circle', { cx: x, cy: y, r: m ? 6 : 5, fill: C.stage, stroke: done ? col : C.dim, 'stroke-width': 1.6 }));
      }
      svg.appendChild(el('text', { x, y: y - (m ? 22 : 18), 'text-anchor': 'middle', class: 'mono', 'font-size': fNum, fill: on ? col : C.faint }, String(i + 1)));
      svg.appendChild(el('text', { x, y: y + (m ? 30 : 24), 'text-anchor': 'middle', 'font-size': fLabel, 'font-weight': on ? 700 : 400, fill: on ? C.text : C.faint }, stop.label));
    });
    // phase 1 / phase 2 bracket captions
    const bx0 = xOf(0), bx1 = xOf(1), bx2 = xOf(2), bx4 = xOf(4);
    const bracket = (x0, x1, label) => {
      const yb = y + (m ? 50 : 40);
      svg.appendChild(el('path', { d: `M ${x0} ${yb - 4} L ${x0} ${yb} L ${x1} ${yb} L ${x1} ${yb - 4}`, fill: 'none', stroke: C.softline, 'stroke-width': 1 }));
      svg.appendChild(el('text', { x: (x0 + x1) / 2, y: yb + (m ? 16 : 12), 'text-anchor': 'middle', 'font-size': fBracket, fill: C.faint }, label));
    };
    bracket(bx0, bx1, 'Phase 1');
    bracket(bx2, bx4, 'Phase 2');
    return svg;
  }

  _committedProg(p) {
    const wrap = document.createElement('div');
    wrap.className = 'paros-multiprog';
    const lead = document.createElement('span');
    lead.textContent = p.leader === null || p.leader === undefined ? 'electing a leader…' : `leader N${p.leader} · r${p.round}`;
    lead.style.color = p.leader === null || p.leader === undefined ? C.muted : PHASE.leader;
    const meter = document.createElement('div');
    meter.className = 'paros-meter';
    const fill = document.createElement('i');
    const frac = p.high >= 0 ? (p.committed + 1) / (p.high + 1) : 0;
    fill.style.width = `${Math.round(frac * 100)}%`;
    meter.appendChild(fill);
    const label = document.createElement('span');
    label.textContent = `committed ${p.committed + 1} / ${p.high + 1} slots`;
    wrap.append(lead, meter, label);
    return wrap;
  }

  _renderDigest() {
    clear(this.digestEl);
    for (const chip of this.renderer.digest(this.run) || []) {
      const c = document.createElement('div');
      c.className = 'paros-chip';
      c.innerHTML = `<b></b><span></span>`;
      c.querySelector('b').textContent = chip.value;
      c.querySelector('span').textContent = chip.label;
      this.digestEl.appendChild(c);
    }
  }

  _renderOracle() {
    clear(this.oracleEl);
    for (const o of oracleBadge(this.run, this.modeId)) {
      const s = document.createElement('span');
      const i = document.createElement('i');
      i.style.background = o.ok ? PHASE.chosen : PHASE.nack;
      s.append(i, document.createTextNode(`${o.ok ? '✓' : '✗'} ${o.label}`));
      this.oracleEl.appendChild(s);
    }
  }

  // ---- layout / responsive -------------------------------------------------
  _onResize() {
    const w = this.root.clientWidth;
    const mobile = w > 0 && w < 620;
    if (mobile !== this.mobile || this._vw === undefined) {
      this.mobile = mobile;
      this.root.classList.toggle('is-mobile', mobile);
      this._layout();
      if (this.frames.length) this.showFrame(this.idx, 1);
    }
  }

  _layout() {
    const size = this.renderer.stageSize
      ? this.renderer.stageSize(this.mobile)
      : { w: 1000, h: this.mobile ? 520 : 380 };
    this._vw = size.w; this._vh = size.h;
    this.svg.setAttribute('viewBox', `0 0 ${size.w} ${size.h}`);
    this.svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');
    // when the stage overflows on mobile, give it a min pixel width so it scrolls
    // (and only then show the swipe hint — a stage that fits has nothing to swipe)
    const swipes = !!(this.mobile && size.scrollW);
    if (swipes) this.svg.style.minWidth = size.scrollW + 'px';
    else this.svg.style.minWidth = '';
    this.svg.parentElement.classList.toggle('can-swipe', swipes);
  }

  _onKey(e) {
    if (e.target === this.seedInput || /input|select|textarea/i.test(e.target.tagName)) return;
    if (this.renderer.transport === 'still') return;
    if (e.key === 'ArrowRight') { e.preventDefault(); this.step(1); }
    else if (e.key === 'ArrowLeft') { e.preventDefault(); this.step(-1); }
    else if (e.key === ' ') { e.preventDefault(); this.togglePlay(); }
  }

  _runSeed() {
    const v = this.seedInput.value || '0';
    this.onModeChange(this.modeId, v); // main.js re-runs the sim for this seed
  }

  destroy() {
    this._stopAnim();
    this.ro.disconnect();
    document.removeEventListener('keydown', this._keyHandler);
  }
}
