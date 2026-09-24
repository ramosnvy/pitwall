'use strict';

// Replay 2D de uma corrida a partir dos dados da OpenF1 preparados por
// tools/race-replay. Posicoes interpoladas entre amostras (~3,7 Hz por carro);
// telemetria mostrada pela ultima amostra recebida, como um pit wall veria.

const $ = (id) => document.getElementById(id);

const RECENT_MS = 8000;      // quanto tempo uma frenagem fica marcada na pista
const TRACE_MS = 30000;      // janela do grafico de telemetria
const GAP_MS = 2500;         // acima disto nao interpola: lacuna nos dados
const OUT_MS = 20000;        // sem posicao por mais que isto: fora da corrida

const state = {
  data: null,
  t: 0,
  playing: false,
  speed: 4,
  marks: 'recent',
  selected: null,
  lastFrame: 0,
  lastPanel: 0,
  dragging: false,
  view: null,
};

// ------------------------------------------------------------------ utilidades

function cumsum(values) {
  const out = new Float64Array(values.length);
  let s = 0;
  for (let i = 0; i < values.length; i++) { s += values[i]; out[i] = s; }
  return out;
}

// Indice do ultimo elemento <= t, ou -1.
function upper(t, arr) {
  let lo = 0, hi = arr.length - 1, ans = -1;
  while (lo <= hi) {
    const m = (lo + hi) >> 1;
    if (arr[m] <= t) { ans = m; lo = m + 1; } else { hi = m - 1; }
  }
  return ans;
}

function fmtClock(ms) {
  const sign = ms < 0 ? '−' : '';
  const s = Math.floor(Math.abs(ms) / 1000);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  const mm = String(m).padStart(2, '0'), ss = String(sec).padStart(2, '0');
  return h > 0 ? `${sign}${h}:${mm}:${ss}` : `${sign}${mm}:${ss}`;
}

const nf = new Intl.NumberFormat('pt-BR');

// ------------------------------------------------------------------ dados

function prepare(raw) {
  const d = {
    session: raw.session,
    start: raw.start,
    raceStart: raw.start + 60000,
    end: raw.end,
    totalLaps: raw.totalLaps,
    threshold: raw.brakeThreshold,
    drivers: new Map(),
    track: [],
    marks: { t: [], x: [], y: [] },
    lapT: [],
    lapMax: [],
  };

  for (const drv of raw.drivers) {
    d.drivers.set(drv.n, { ...drv, loc: null, tel: null, brakes: new Float64Array(0), bx: [], by: [], posT: [], posP: [], lastLoc: -Infinity });
  }

  for (const [n, l] of Object.entries(raw.loc)) {
    const drv = d.drivers.get(+n);
    if (!drv) continue;
    drv.loc = { t: cumsum(l.t), x: cumsum(l.x), y: cumsum(l.y) };
    drv.lastLoc = drv.loc.t[drv.loc.t.length - 1];
  }

  for (const [n, s] of Object.entries(raw.tel)) {
    const drv = d.drivers.get(+n);
    if (!drv) continue;
    drv.tel = { t: cumsum(s.t), speed: s.speed, rpm: s.rpm, gear: s.gear, throttle: s.throttle, brake: s.brake, drs: s.drs };
  }

  // Frenagens: instante vindo do Processing.Core; posicao na pista
  // interpolada naquele instante.
  const all = [];
  for (const [n, b] of Object.entries(raw.brakings)) {
    const drv = d.drivers.get(+n);
    if (!drv) continue;
    drv.brakes = cumsum(b);
    for (const t of drv.brakes) {
      const p = locAt(drv, t);
      drv.bx.push(p ? p.x : NaN);
      drv.by.push(p ? p.y : NaN);
      if (p) all.push([t, p.x, p.y]);
    }
  }
  all.sort((a, b) => a[0] - b[0]);
  for (const [t, x, y] of all) { d.marks.t.push(t); d.marks.x.push(x); d.marks.y.push(y); }
  d.marks.t = Float64Array.from(d.marks.t);

  for (let i = 0; i < raw.positions.length; i += 3) {
    const drv = d.drivers.get(raw.positions[i + 1]);
    if (!drv) continue;
    drv.posT.push(raw.positions[i]);
    drv.posP.push(raw.positions[i + 2]);
  }

  // Volta do lider: maior numero de volta iniciado ate o instante.
  let max = 0;
  for (let i = 0; i < raw.laps.length; i += 3) {
    max = Math.max(max, raw.laps[i + 2]);
    d.lapT.push(raw.laps[i]);
    d.lapMax.push(max);
    const drv = d.drivers.get(raw.laps[i + 1]);
    if (drv) drv.lapCount = Math.max(drv.lapCount ?? 0, raw.laps[i + 2]);
  }

  for (let i = 0; i < raw.track.length; i += 2) d.track.push([raw.track[i], raw.track[i + 1]]);

  // Enquadramento: pista mais uma amostra das posicoes, para o box caber.
  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  const grow = (x, y) => { minX = Math.min(minX, x); maxX = Math.max(maxX, x); minY = Math.min(minY, y); maxY = Math.max(maxY, y); };
  for (const [x, y] of d.track) grow(x, y);
  for (const drv of d.drivers.values()) {
    if (!drv.loc) continue;
    for (let i = 0; i < drv.loc.t.length; i += 97) {
      if (drv.loc.t[i] > d.raceStart) grow(drv.loc.x[i], drv.loc.y[i]);
    }
  }
  d.bbox = { minX, maxX, minY, maxY };

  return d;
}

function locAt(drv, t) {
  const loc = drv.loc;
  if (!loc) return null;
  const i = upper(t, loc.t);
  if (i < 0) return null;
  if (t - drv.lastLoc > OUT_MS) return null;
  if (i === loc.t.length - 1) return { x: loc.x[i], y: loc.y[i] };
  const t0 = loc.t[i], t1 = loc.t[i + 1];
  if (t1 - t0 > GAP_MS) return { x: loc.x[i], y: loc.y[i] };
  const k = (t - t0) / (t1 - t0);
  return { x: loc.x[i] + (loc.x[i + 1] - loc.x[i]) * k, y: loc.y[i] + (loc.y[i + 1] - loc.y[i]) * k };
}

function positionAt(drv, t) {
  const i = upper(t, drv.posT);
  return i < 0 ? null : drv.posP[i];
}

function leaderLap(d, t) {
  const i = upper(t, d.lapT);
  return i < 0 ? 0 : d.lapMax[i];
}

// ------------------------------------------------------------------ pista

const trackCanvas = $('track');
const tctx = trackCanvas.getContext('2d');

function resizeCanvas(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const w = Math.max(1, Math.round(canvas.clientWidth * dpr));
  const h = Math.max(1, Math.round(canvas.clientHeight * dpr));
  if (canvas.width !== w || canvas.height !== h) { canvas.width = w; canvas.height = h; }
  return dpr;
}

function computeView() {
  const d = state.data;
  const dpr = resizeCanvas(trackCanvas);
  const W = trackCanvas.width, H = trackCanvas.height, pad = 40 * dpr;
  const { minX, maxX, minY, maxY } = d.bbox;
  const scale = Math.min((W - 2 * pad) / (maxX - minX), (H - 2 * pad) / (maxY - minY));
  const offX = (W - (maxX - minX) * scale) / 2;
  const offY = (H - (maxY - minY) * scale) / 2;
  state.view = {
    dpr,
    sx: (x) => offX + (x - minX) * scale,
    sy: (y) => H - (offY + (y - minY) * scale),   // y da OpenF1 cresce para cima
  };
}

function drawTrack() {
  const d = state.data, v = state.view;
  const W = trackCanvas.width, H = trackCanvas.height, dpr = v.dpr;
  tctx.clearRect(0, 0, W, H);

  // Asfalto e linha central.
  if (d.track.length > 1) {
    tctx.lineJoin = 'round'; tctx.lineCap = 'round';
    tctx.beginPath();
    d.track.forEach(([x, y], i) => (i ? tctx.lineTo(v.sx(x), v.sy(y)) : tctx.moveTo(v.sx(x), v.sy(y))));
    tctx.closePath();
    tctx.strokeStyle = '#222B34'; tctx.lineWidth = 16 * dpr; tctx.stroke();
    tctx.strokeStyle = '#3A4652'; tctx.lineWidth = 1.5 * dpr; tctx.stroke();
  }

  const t = state.t;

  // Frenagens detectadas.
  if (state.marks === 'all') {
    const n = upper(t, d.marks.t) + 1;
    tctx.fillStyle = 'rgba(255, 77, 90, 0.22)';
    const s = 2.5 * dpr;
    for (let i = 0; i < n; i++) tctx.fillRect(v.sx(d.marks.x[i]) - s / 2, v.sy(d.marks.y[i]) - s / 2, s, s);
  } else if (state.marks === 'recent') {
    const hi = upper(t, d.marks.t);
    for (let i = hi; i >= 0 && t - d.marks.t[i] < RECENT_MS; i--) {
      const a = 1 - (t - d.marks.t[i]) / RECENT_MS;
      tctx.fillStyle = `rgba(255, 77, 90, ${0.15 + 0.75 * a})`;
      tctx.beginPath();
      tctx.arc(v.sx(d.marks.x[i]), v.sy(d.marks.y[i]), (2 + 3 * a) * dpr, 0, Math.PI * 2);
      tctx.fill();
    }
  }

  // Carros: selecionado por ultimo, para ficar por cima.
  const cars = [];
  for (const drv of d.drivers.values()) {
    const p = locAt(drv, t);
    if (p) cars.push([drv, p]);
  }
  cars.sort((a, b) => (a[0] === state.selected) - (b[0] === state.selected));
  state.onTrack = cars;

  tctx.font = `600 ${12 * dpr}px "Barlow Condensed", "Arial Narrow", sans-serif`;
  tctx.textBaseline = 'middle';
  for (const [drv, p] of cars) {
    const x = v.sx(p.x), y = v.sy(p.y), sel = drv === state.selected;
    if (sel) {
      tctx.strokeStyle = '#F2C14E'; tctx.lineWidth = 2 * dpr;
      tctx.beginPath(); tctx.arc(x, y, 11 * dpr, 0, Math.PI * 2); tctx.stroke();
    }
    tctx.fillStyle = drv.color;
    tctx.strokeStyle = '#0E1216'; tctx.lineWidth = 2 * dpr;
    tctx.beginPath(); tctx.arc(x, y, 6.5 * dpr, 0, Math.PI * 2); tctx.fill(); tctx.stroke();
    tctx.fillStyle = sel ? '#F2C14E' : '#C9D1D6';
    tctx.fillText(drv.code, x + 10 * dpr, y - 9 * dpr);
  }
}

trackCanvas.addEventListener('click', (e) => {
  if (!state.onTrack) return;
  const r = trackCanvas.getBoundingClientRect(), v = state.view;
  const mx = (e.clientX - r.left) * v.dpr, my = (e.clientY - r.top) * v.dpr;
  let best = null, bestD = (18 * v.dpr) ** 2;
  for (const [drv, p] of state.onTrack) {
    const dx = v.sx(p.x) - mx, dy = v.sy(p.y) - my, dd = dx * dx + dy * dy;
    if (dd < bestD) { best = drv; bestD = dd; }
  }
  if (best) select(best);
});

// ------------------------------------------------------------------ paineis

const traceCanvas = $('trace');
const rctx = traceCanvas.getContext('2d');

function select(drv) {
  state.selected = drv;
  $('car-color').style.background = drv.color;
  $('car-code').textContent = drv.code;
  $('car-name').textContent = drv.team ? `${drv.name} · ${drv.team}` : drv.name;
  updateBoard(true);
  updateCar();
}

function updateBoard(force) {
  const d = state.data, t = state.t;
  const rows = [...d.drivers.values()].map((drv) => ({
    drv,
    pos: positionAt(drv, t) ?? 99,
    out: !drv.loc || t - drv.lastLoc > OUT_MS,
    brakes: upper(t, drv.brakes) + 1,
  }));
  rows.sort((a, b) => (a.out - b.out) || (a.pos - b.pos) || (a.drv.n - b.drv.n));

  const key = rows.map((r) => `${r.drv.n}:${r.pos}:${r.out}:${r.brakes}`).join('|') + (state.selected?.n ?? '');
  if (!force && key === state.boardKey) return;
  state.boardKey = key;

  const board = $('board');
  board.replaceChildren(...rows.map((r) => {
    const li = document.createElement('li');
    li.className = (r.out ? 'out ' : '') + (r.drv === state.selected ? 'sel' : '');
    li.innerHTML = `<span class="p">${r.out ? 'OUT' : r.pos === 99 ? '–' : r.pos}</span>` +
      `<span class="who"><i style="background:${r.drv.color}"></i><b>${r.drv.code}</b><small>${r.drv.team}</small></span>` +
      `<span class="n">${nf.format(r.brakes)}</span>`;
    li.addEventListener('click', () => select(r.drv));
    return li;
  }));
}

function updateCar() {
  const drv = state.selected, d = state.data, t = state.t;
  if (!drv) return;
  const pos = positionAt(drv, t);
  $('car-pos').textContent = pos ? `P${pos}` : '';

  const tel = drv.tel;
  const i = tel ? upper(t, tel.t) : -1;
  if (i < 0) {
    for (const id of ['speed', 'gear', 'rpm', 'thr', 'brk']) $(id).textContent = '–';
  } else {
    const rpm = tel.rpm[i], thr = tel.throttle[i], brk = tel.brake[i];
    $('speed').textContent = tel.speed[i];
    $('gear').textContent = tel.gear[i] || 'N';
    $('rpm').textContent = nf.format(rpm);
    $('thr').textContent = `${thr}%`;
    $('brk').textContent = brk;
    $('rpm-bar').style.width = `${Math.min(100, rpm / 130)}%`;
    $('thr-bar').style.width = `${Math.min(100, thr)}%`;
    $('brk-bar').style.width = `${Math.min(100, brk)}%`;
    // DRS na OpenF1: 10, 12 e 14 = asa aberta; 8 = elegivel.
    $('drs').classList.toggle('on', tel.drs[i] >= 10);
  }

  $('car-brakes').textContent = nf.format(upper(t, drv.brakes) + 1);
  $('car-samples').textContent = nf.format(i + 1);
  drawTrace(drv, t);
}

function drawTrace(drv, t) {
  const dpr = resizeCanvas(traceCanvas);
  const W = traceCanvas.width, H = traceCanvas.height;
  rctx.clearRect(0, 0, W, H);
  const tel = drv.tel;
  if (!tel) return;

  const from = t - TRACE_MS;
  const x = (tt) => ((tt - from) / TRACE_MS) * W;
  const y = (speed) => H - 6 * dpr - (speed / 360) * (H - 12 * dpr);
  const lo = Math.max(0, upper(from, tel.t)), hi = upper(t, tel.t);

  // Faixas de freio acima do limiar.
  rctx.fillStyle = 'rgba(255, 77, 90, 0.28)';
  for (let i = lo; i <= hi && i < tel.t.length - 1; i++) {
    if (tel.brake[i] >= state.data.threshold) rctx.fillRect(x(tel.t[i]), 0, Math.max(1, x(tel.t[i + 1]) - x(tel.t[i])), H);
  }

  // Grade de 100 em 100 km/h.
  rctx.strokeStyle = 'rgba(255,255,255,0.06)'; rctx.lineWidth = 1;
  for (const s of [100, 200, 300]) { rctx.beginPath(); rctx.moveTo(0, y(s)); rctx.lineTo(W, y(s)); rctx.stroke(); }

  // Velocidade.
  rctx.strokeStyle = '#E6EAE8'; rctx.lineWidth = 1.6 * dpr;
  rctx.beginPath();
  for (let i = lo; i <= hi; i++) (i === lo ? rctx.moveTo : rctx.lineTo).call(rctx, x(tel.t[i]), y(tel.speed[i]));
  rctx.stroke();

  // Frenagens detectadas pelo Processing.Core.
  rctx.fillStyle = '#F2C14E';
  const b = drv.brakes;
  for (let i = upper(t, b); i >= 0 && b[i] >= from; i--) rctx.fillRect(x(b[i]) - dpr, 0, 2 * dpr, 10 * dpr);
}

function updateHeader() {
  const d = state.data, t = state.t;
  const lap = leaderLap(d, t);
  $('lap').textContent = lap > 0 ? `${lap} / ${d.totalLaps}` : `– / ${d.totalLaps}`;
  $('clock').textContent = fmtClock(t - d.raceStart);
  if (!state.dragging) $('scrub').value = Math.round(((t - d.start) / (d.end - d.start)) * 1000);
}

// ------------------------------------------------------------------ laco

function frame(now) {
  const d = state.data;
  if (d && state.viewMode !== 'stats') {
    const dt = state.lastFrame ? now - state.lastFrame : 0;
    if (state.playing) {
      state.t = Math.min(d.end, state.t + dt * state.speed);
      if (state.t >= d.end) setPlaying(false);
    }
    drawTrack();
    if (now - state.lastPanel > 66) {   // paineis a ~15 quadros/s bastam
      updateHeader();
      updateCar();
      updateBoard(false);
      state.lastPanel = now;
    }
  }
  state.lastFrame = now;
  requestAnimationFrame(frame);
}

function setPlaying(on) {
  state.playing = on;
  const b = $('play');
  b.textContent = on ? '❚❚' : '▶';
  b.setAttribute('aria-label', on ? 'Pausar' : 'Reproduzir');
}

// ------------------------------------------------------------------ controles

$('play').addEventListener('click', () => setPlaying(!state.playing));

document.querySelectorAll('[data-speed]').forEach((b) => b.addEventListener('click', () => {
  state.speed = +b.dataset.speed;
  document.querySelectorAll('[data-speed]').forEach((o) => o.classList.toggle('on', o === b));
}));

document.querySelectorAll('[data-marks]').forEach((b) => b.addEventListener('click', () => {
  state.marks = b.dataset.marks;
  document.querySelectorAll('[data-marks]').forEach((o) => o.classList.toggle('on', o === b));
}));

const scrub = $('scrub');
scrub.addEventListener('input', () => {
  const d = state.data;
  if (!d) return;
  state.dragging = true;
  state.t = d.start + (scrub.value / 1000) * (d.end - d.start);
});
scrub.addEventListener('change', () => { state.dragging = false; });

document.addEventListener('keydown', (e) => {
  if (!state.data || e.target.tagName === 'SELECT') return;
  if (e.code === 'Space') { e.preventDefault(); setPlaying(!state.playing); }
  if (e.code === 'ArrowRight') state.t = Math.min(state.data.end, state.t + 10000);
  if (e.code === 'ArrowLeft') state.t = Math.max(state.data.start, state.t - 10000);
});

window.addEventListener('resize', () => state.data && computeView());

$('session').addEventListener('change', (e) => {
  history.replaceState(null, '', `#${e.target.value}`);
  loadSession(e.target.value);
});

window.addEventListener('hashchange', () => {
  const key = location.hash.slice(1);
  if (index.some((s) => String(s.key) === key) && String(state.data?.session.key) !== key) {
    $('session').value = key;
    loadSession(key);
  }
});

// ------------------------------------------------------------------ estatisticas

// Tudo calculado aqui, a partir dos dados ja carregados para o replay. As
// frenagens vem prontas do Processing.Core (tools/race-replay); o resto e
// contagem sobre as amostras.

const INTERVAL_BIN_MS = 25;
const INTERVAL_BINS = 40;        // 0 a 1000 ms; um balde extra para lacunas
const SPEED_BIN = 10;
const SPEED_BINS = 37;           // 0 a 360 km/h
const LANES = 4;                 // faixas da matriz

function css(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }

function computeStats(d) {
  const minutes = Math.ceil(d.end / 60000) + 1;
  const intervals = new Array(INTERVAL_BINS + 1).fill(0);
  const speed = new Array(SPEED_BINS).fill(0);
  const samplesPerMin = new Array(minutes).fill(0);
  const brakesPerMin = new Array(minutes).fill(0);
  const rows = [];
  let tel = 0, loc = 0, gaps = 0, brakesRace = 0, brakesAll = 0, tMin = Infinity, tMax = -Infinity;

  for (const drv of d.drivers.values()) {
    const s = drv.tel;
    if (!s) continue;
    const n = s.t.length;
    tel += n;
    loc += drv.loc ? drv.loc.t.length : 0;
    tMin = Math.min(tMin, s.t[0]);
    tMax = Math.max(tMax, s.t[n - 1]);

    let carGaps = 0, sumDt = 0, cntDt = 0, max = 0, sumSpeed = 0, race = 0, braking = 0, drs = 0;
    for (let i = 0; i < n; i++) {
      const t = s.t[i];
      samplesPerMin[Math.floor(t / 60000)]++;
      if (i > 0) {
        const dt = t - s.t[i - 1];
        if (dt > 1000) { carGaps++; intervals[INTERVAL_BINS]++; }
        else { intervals[Math.min(INTERVAL_BINS - 1, Math.floor(dt / INTERVAL_BIN_MS))]++; sumDt += dt; cntDt++; }
      }
      if (t >= d.raceStart) {
        const v = s.speed[i];
        race++; sumSpeed += v; if (v > max) max = v;
        speed[Math.min(SPEED_BINS - 1, Math.floor(v / SPEED_BIN))]++;
        if (s.brake[i] >= d.threshold) braking++;
        if (s.drs[i] >= 10) drs++;       // 10, 12, 14 = asa aberta
      }
    }

    let carBrakes = 0;
    for (const b of drv.brakes) {
      brakesPerMin[Math.floor(b / 60000)]++;
      if (b >= d.raceStart) carBrakes++;
    }
    brakesRace += carBrakes;
    brakesAll += drv.brakes.length;
    gaps += carGaps;

    rows.push({
      drv, n,
      hz: cntDt > 0 ? 1000 / (sumDt / cntDt) : 0,
      gaps: carGaps,
      maxSpeed: max,
      avgSpeed: race ? sumSpeed / race : 0,
      brakes: carBrakes,
      laps: drv.lapCount ?? 0,
      perLap: drv.lapCount ? carBrakes / drv.lapCount : NaN,
      brakePct: race ? 100 * braking / race : 0,
      drsPct: race ? 100 * drs / race : 0,
      finalPos: drv.posP.length ? drv.posP[drv.posP.length - 1] : 99,
    });
  }

  // Mediana do intervalo pelo histograma (centro do balde).
  const inside = intervals.slice(0, INTERVAL_BINS).reduce((a, b) => a + b, 0);
  let acc = 0, medianMs = NaN;
  for (let i = 0; i < INTERVAL_BINS; i++) {
    acc += intervals[i];
    if (acc >= inside / 2) { medianMs = (i + 0.5) * INTERVAL_BIN_MS; break; }
  }
  const upTo300 = intervals.slice(0, Math.ceil(300 / INTERVAL_BIN_MS)).reduce((a, b) => a + b, 0);
  const cars = rows.length;

  return {
    cars, tel, loc, gaps, brakesRace, brakesAll, rows, intervals, speed, samplesPerMin, brakesPerMin,
    medianMs,
    meanHz: rows.reduce((a, r) => a + r.hz, 0) / Math.max(1, cars),
    pctUpTo300: 100 * upTo300 / Math.max(1, inside + intervals[INTERVAL_BINS]),
    // Mesma definicao do replayer: amostras / duracao da telemetria.
    naturalRate: tel / ((tMax - tMin) / 1000),
  };
}

function card(value, label) {
  return `<div class="st-card"><b>${value}</b><span>${label}</span></div>`;
}

function renderStats() {
  const d = state.data;
  if (!d) return;
  if (!state.stats || state.stats.key !== d.session.key) {
    state.stats = { key: d.session.key, ...computeStats(d) };
  }
  const st = state.stats;
  const dur = d.end / 60000;

  $('st-title').textContent = `Estatísticas · ${d.session.circuit} ${d.session.year}`;
  $('st-cards').innerHTML =
    card(nf.format(st.tel), 'amostras de telemetria') +
    card(nf.format(st.loc), 'amostras de posição') +
    card(st.cars, 'carros') +
    card(`${Math.floor(dur / 60)}h${String(Math.round(dur % 60)).padStart(2, '0')}`, 'duração da sessão') +
    card(d.totalLaps, 'voltas') +
    card(nf.format(st.brakesRace), `frenagens após a largada (${nf.format(st.brakesAll)} na sessão)`) +
    card(st.meanHz.toFixed(2).replace('.', ',') + ' Hz', 'cadência média por carro') +
    card(nf.format(st.gaps), 'lacunas acima de 1 s');

  $('st-cadence').innerHTML = [
    ['intervalo mediano', `${Math.round(st.medianMs)} ms`],
    ['cadência média', `${st.meanHz.toFixed(2).replace('.', ',')} Hz`],
    ['intervalos até 300 ms', `${st.pctUpTo300.toFixed(1).replace('.', ',')}%`],
    ['taxa natural da corrida', `${Math.round(st.naturalRate)} ev/s`],
  ].map(([k, v]) => `<div><dt>${k}</dt><dd>${v}</dd></div>`).join('');

  drawBars($('st-intervals'), st.intervals, {
    color: css('--rpm'), overflowColor: css('--brake'),
    label: (i) => (i === INTERVAL_BINS ? '>1 s' : `${i * INTERVAL_BIN_MS}`),
    every: 4, xTitle: 'ms entre amostras',
  });
  drawBars($('st-speed'), st.speed, {
    color: css('--throttle'), label: (i) => `${i * SPEED_BIN}`, every: 4, xTitle: 'km/h',
  });
  drawLines($('st-timeline'), [
    { values: st.samplesPerMin, color: css('--rpm'), axis: 'left' },
    { values: st.brakesPerMin, color: css('--brake'), axis: 'right' },
  ], { raceStartMin: d.raceStart / 60000 });

  renderCalc();
  renderDriverTable();
}

function renderCalc() {
  const st = state.stats;
  if (!st) return;
  const rate = +$('st-rate').value;
  const fmt = (x) => nf.format(Math.round(x));

  const native = { hz: st.meanHz, fleet: Math.max(1, Math.round(rate / st.naturalRate)) };
  native.cars = st.cars * native.fleet;
  const hi = { hz: 100, fleet: Math.max(1, Math.round(rate / (st.cars * 100))) };
  hi.cars = st.cars * hi.fleet;

  const rows = [
    ['Frequência por carro', `${native.hz.toFixed(1).replace('.', ',')} Hz (medido)`, '100 Hz (interpolado)'],
    ['Fator de frota', `×${fmt(native.fleet)}`, `×${fmt(hi.fleet)}`],
    ['Carros distintos (chaves)', fmt(native.cars), fmt(hi.cars)],
    ['Janelas de 1 s gravadas por segundo', fmt(native.cars), fmt(hi.cars)],
    ['Eventos por janela por carro', `~${native.hz.toFixed(1).replace('.', ',')}`, '~100'],
    [`Taxa por faixa (${LANES} faixas)`, `${fmt(rate / LANES)} ev/s`, `${fmt(rate / LANES)} ev/s`],
    ['Intervalo entre amostras de um carro', `~${Math.round(1000 / native.hz)} ms`, '10 ms'],
  ];
  $('st-calc').innerHTML =
    '<thead><tr><th></th><th>3,7 Hz</th><th>100 Hz</th></tr></thead><tbody>' +
    rows.map((r) => `<tr><td class="l">${r[0]}</td><td>${r[1]}</td><td>${r[2]}</td></tr>`).join('') +
    '</tbody>';
}

const DRIVER_COLUMNS = [
  { key: 'finalPos', label: 'Pos', fmt: (r) => (r.finalPos === 99 ? '–' : r.finalPos) },
  { key: 'code', label: 'Piloto', fmt: (r) => `<span class="who"><i style="background:${r.drv.color}"></i><b>${r.drv.code}</b></span>`, sort: (r) => r.drv.code, left: true },
  { key: 'team', label: 'Equipe', fmt: (r) => r.drv.team, sort: (r) => r.drv.team, left: true },
  { key: 'n', label: 'Amostras', fmt: (r) => nf.format(r.n) },
  { key: 'hz', label: 'Hz', fmt: (r) => r.hz.toFixed(2).replace('.', ',') },
  { key: 'gaps', label: 'Lacunas >1 s', fmt: (r) => r.gaps },
  { key: 'laps', label: 'Voltas', fmt: (r) => r.laps },
  { key: 'maxSpeed', label: 'Vel. máx', fmt: (r) => `${r.maxSpeed} km/h` },
  { key: 'avgSpeed', label: 'Vel. média', fmt: (r) => `${Math.round(r.avgSpeed)} km/h` },
  { key: 'brakes', label: 'Frenagens', fmt: (r) => nf.format(r.brakes) },
  { key: 'perLap', label: 'Fren./volta', fmt: (r) => (Number.isFinite(r.perLap) ? r.perLap.toFixed(1).replace('.', ',') : '–') },
  { key: 'brakePct', label: '% freando', fmt: (r) => `${r.brakePct.toFixed(1).replace('.', ',')}%<span class="st-bar" style="width:${Math.round(r.brakePct * 2)}px"></span>` },
  { key: 'drsPct', label: '% DRS', fmt: (r) => `${r.drsPct.toFixed(1).replace('.', ',')}%` },
];

function renderDriverTable() {
  const st = state.stats;
  const sortKey = state.statsSort ?? 'finalPos';
  const dir = state.statsDir ?? 1;
  const col = DRIVER_COLUMNS.find((c) => c.key === sortKey) ?? DRIVER_COLUMNS[0];
  const value = col.sort ?? ((r) => r[col.key]);
  const rows = [...st.rows].sort((a, b) => {
    const va = value(a), vb = value(b);
    return (va > vb ? 1 : va < vb ? -1 : 0) * dir;
  });

  const table = $('st-drivers');
  table.innerHTML =
    '<thead><tr>' + DRIVER_COLUMNS.map((c) =>
      `<th data-key="${c.key}" aria-sort="${c.key === sortKey ? (dir > 0 ? 'ascending' : 'descending') : 'none'}">${c.label}${c.key === sortKey ? (dir > 0 ? ' ▲' : ' ▼') : ''}</th>`).join('') +
    '</tr></thead><tbody>' +
    rows.map((r) => '<tr>' + DRIVER_COLUMNS.map((c) => `<td class="${c.left ? 'l' : ''}">${c.fmt(r)}</td>`).join('') + '</tr>').join('') +
    '</tbody>';

  table.querySelectorAll('th').forEach((th) => th.addEventListener('click', () => {
    const key = th.dataset.key;
    state.statsDir = state.statsSort === key ? -(state.statsDir ?? 1) : 1;
    state.statsSort = key;
    renderDriverTable();
  }));
}

// Histograma: um balde por barra, rotulo a cada "every" baldes.
function drawBars(canvas, bins, opt) {
  const dpr = resizeCanvas(canvas);
  const ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
  const padL = 44 * dpr, padR = 10 * dpr, padT = 10 * dpr, padB = 34 * dpr;
  const max = Math.max(1, ...bins);
  const bw = (W - padL - padR) / bins.length;
  ctx.clearRect(0, 0, W, H);

  gridY(ctx, max, padL, padR, padT, padB, W, H, dpr, (v) => nf.format(Math.round(v)));

  bins.forEach((v, i) => {
    const h = (v / max) * (H - padT - padB);
    ctx.fillStyle = opt.overflowColor && i === bins.length - 1 ? opt.overflowColor : opt.color;
    ctx.fillRect(padL + i * bw + dpr, H - padB - h, Math.max(1, bw - 2 * dpr), h);
  });

  ctx.fillStyle = css('--muted');
  ctx.font = `${10 * dpr}px ${css('--mono') || 'monospace'}`;
  ctx.textAlign = 'center';
  bins.forEach((_, i) => {
    if (i % opt.every === 0 || i === bins.length - 1) ctx.fillText(opt.label(i), padL + (i + 0.5) * bw, H - padB + 14 * dpr);
  });
  ctx.fillText(opt.xTitle, padL + (W - padL - padR) / 2, H - 6 * dpr);
}

// Linhas por minuto, com eixo proprio para a segunda serie.
function drawLines(canvas, series, opt) {
  const dpr = resizeCanvas(canvas);
  const ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
  const padL = 50 * dpr, padR = 42 * dpr, padT = 10 * dpr, padB = 30 * dpr;
  const n = series[0].values.length;
  const x = (i) => padL + (i / Math.max(1, n - 1)) * (W - padL - padR);
  ctx.clearRect(0, 0, W, H);

  const maxL = Math.max(1, ...series.filter((s) => s.axis === 'left').flatMap((s) => s.values));
  const maxR = Math.max(1, ...series.filter((s) => s.axis === 'right').flatMap((s) => s.values));
  gridY(ctx, maxL, padL, padR, padT, padB, W, H, dpr, (v) => nf.format(Math.round(v)));

  // Largada.
  if (opt.raceStartMin > 0) {
    ctx.strokeStyle = css('--accent'); ctx.setLineDash([4 * dpr, 4 * dpr]); ctx.lineWidth = dpr;
    const xs = x(opt.raceStartMin);
    ctx.beginPath(); ctx.moveTo(xs, padT); ctx.lineTo(xs, H - padB); ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle = css('--accent'); ctx.font = `${10 * dpr}px ${css('--sans') || 'sans-serif'}`; ctx.textAlign = 'left';
    ctx.fillText('largada', xs + 4 * dpr, padT + 10 * dpr);
  }

  for (const s of series) {
    const max = s.axis === 'right' ? maxR : maxL;
    ctx.strokeStyle = s.color; ctx.lineWidth = 1.6 * dpr;
    ctx.beginPath();
    s.values.forEach((v, i) => {
      const y = H - padB - (v / max) * (H - padT - padB);
      i ? ctx.lineTo(x(i), y) : ctx.moveTo(x(i), y);
    });
    ctx.stroke();
  }

  // Eixo da direita e eixo do tempo.
  ctx.fillStyle = css('--brake'); ctx.font = `${10 * dpr}px ${css('--mono') || 'monospace'}`; ctx.textAlign = 'left';
  for (let k = 0; k <= 4; k++) {
    const v = (maxR * k) / 4;
    ctx.fillText(nf.format(Math.round(v)), W - padR + 4 * dpr, H - padB - (k / 4) * (H - padT - padB) + 3 * dpr);
  }
  ctx.fillStyle = css('--muted'); ctx.textAlign = 'center';
  for (let m = 0; m < n; m += 15) ctx.fillText(`${m} min`, x(m), H - padB + 14 * dpr);
}

function gridY(ctx, max, padL, padR, padT, padB, W, H, dpr, fmt) {
  ctx.strokeStyle = 'rgba(255,255,255,0.07)'; ctx.lineWidth = 1;
  ctx.fillStyle = css('--muted'); ctx.font = `${10 * dpr}px ${css('--mono') || 'monospace'}`; ctx.textAlign = 'right';
  for (let k = 0; k <= 4; k++) {
    const y = H - padB - (k / 4) * (H - padT - padB);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W - padR, y); ctx.stroke();
    ctx.fillText(fmt((max * k) / 4), padL - 6 * dpr, y + 3 * dpr);
  }
}

function setView(view) {
  state.viewMode = view;
  $('stats').hidden = view !== 'stats';
  document.querySelector('main.layout').hidden = view === 'stats';
  document.querySelector('footer.transport').hidden = view === 'stats';
  document.querySelectorAll('.tabs button').forEach((b) => b.setAttribute('aria-selected', String(b.dataset.view === view)));
  if (view === 'stats') { setPlaying(false); renderStats(); }
  else if (state.data) computeView();
  try { localStorage.setItem('pitwall-replay-view', view); } catch { /* sem armazenamento: tudo bem */ }
}

document.querySelectorAll('.tabs button').forEach((b) => b.addEventListener('click', () => setView(b.dataset.view)));
$('st-rate').addEventListener('change', renderCalc);
window.addEventListener('resize', () => state.viewMode === 'stats' && renderStats());

// ------------------------------------------------------------------ carga

let index = [];

async function loadSession(key) {
  const entry = index.find((s) => String(s.key) === String(key)) ?? index[0];
  setPlaying(false);
  $('status').textContent = `Carregando ${entry.country}…`;

  const response = await fetch(`data/${entry.file}`);
  if (!response.ok) { $('status').textContent = `Falha ao carregar data/${entry.file} (${response.status}). Rode tools/race-replay primeiro.`; return; }

  const data = prepare(await response.json());
  state.data = data;
  state.t = data.start;
  $('threshold').textContent = data.threshold;
  computeView();

  const samples = [...data.drivers.values()].reduce((s, drv) => s + (drv.tel ? drv.tel.t.length : 0), 0);
  $('status').textContent =
    `${data.session.circuit} ${data.session.year} · ${data.drivers.size} carros · ${nf.format(samples)} amostras · ${nf.format(data.marks.t.length)} frenagens`;

  // Comeca com o lider do grid selecionado.
  const first = [...data.drivers.values()].sort((a, b) => (positionAt(a, data.raceStart) ?? 99) - (positionAt(b, data.raceStart) ?? 99))[0];
  select(first);
  updateHeader();
  if (state.viewMode === 'stats') renderStats();
}

async function init() {
  try {
    const response = await fetch('data/index.json');
    index = await response.json();
  } catch {
    $('status').textContent = 'Sem dados: rode tools/race-replay para gerar web/data.';
    return;
  }

  const select = $('session');
  select.replaceChildren(...index.map((s) => new Option(`${s.country} ${s.year} · ${s.circuit}`, s.key)));
  const wanted = location.hash.slice(1) || '9472';
  select.value = index.some((s) => String(s.key) === wanted) ? wanted : index[0].key;
  await loadSession(select.value);
  let saved = null;
  try { saved = localStorage.getItem('pitwall-replay-view'); } catch { /* sem armazenamento */ }
  if (saved === 'stats') setView('stats');
  requestAnimationFrame(frame);
}

init();
