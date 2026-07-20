/* Натальная карта · женщина · 09.04.1986 */
const CHART = {
  birth: {
    date: "09.04.1986",
    note: "12:00 UT; время и место рождения не указаны",
  },
  planets: [
    { id: "Sun", name: "Солнце", glyph: "☉", lon: 19.341, sign: "Овен", text: "19°20′", retro: false, element: "fire" },
    { id: "Moon", name: "Луна", glyph: "☽", lon: 22.0923, sign: "Овен", text: "22°06′", retro: false, element: "fire" },
    { id: "Mercury", name: "Меркурий", glyph: "☿", lon: 352.1626, sign: "Рыбы", text: "22°10′", retro: false, element: "water" },
    { id: "Venus", name: "Венера", glyph: "♀", lon: 38.8131, sign: "Телец", text: "8°49′", retro: false, element: "earth" },
    { id: "Mars", name: "Марс", glyph: "♂", lon: 276.026, sign: "Козерог", text: "6°02′", retro: false, element: "earth" },
    { id: "Jupiter", name: "Юпитер", glyph: "♃", lon: 341.0515, sign: "Рыбы", text: "11°03′", retro: false, element: "water" },
    { id: "Saturn", name: "Сатурн", glyph: "♄", lon: 249.3399, sign: "Стрелец", text: "9°20′", retro: true, element: "fire" },
    { id: "Uranus", name: "Уран", glyph: "♅", lon: 262.3007, sign: "Стрелец", text: "22°18′", retro: true, element: "fire" },
    { id: "Neptune", name: "Нептун", glyph: "♆", lon: 275.807, sign: "Козерог", text: "5°48′", retro: true, element: "earth" },
    { id: "Pluto", name: "Плутон", glyph: "♇", lon: 216.4455, sign: "Скорпион", text: "6°27′", retro: true, element: "water" },
    { id: "Node", name: "Сев. узел", glyph: "☊", lon: 0.6167, sign: "Овен", text: "0°37′", retro: true, element: "fire" },
    { id: "Chiron", name: "Хирон", glyph: "⚷", lon: 70.85, sign: "Близнецы", text: "10°51′", retro: false, element: "air" },
  ],
  aspects: [
    { a: "Mars", b: "Neptune", aName: "Марс", bName: "Нептун", type: "соединение", symbol: "☌", orb: 0.2 },
    { a: "Moon", b: "Uranus", aName: "Луна", bName: "Уран", type: "трин", symbol: "△", orb: 0.2 },
    { a: "Mercury", b: "Uranus", aName: "Меркурий", bName: "Уран", type: "квадрат", symbol: "□", orb: 0.1 },
    { a: "Mars", b: "Pluto", aName: "Марс", bName: "Плутон", type: "секстиль", symbol: "⚹", orb: 0.4 },
    { a: "Neptune", b: "Pluto", aName: "Нептун", bName: "Плутон", type: "секстиль", symbol: "⚹", orb: 0.6 },
    { a: "Jupiter", b: "Saturn", aName: "Юпитер", bName: "Сатурн", type: "квадрат", symbol: "□", orb: 1.7 },
    { a: "Venus", b: "Jupiter", aName: "Венера", bName: "Юпитер", type: "секстиль", symbol: "⚹", orb: 2.2 },
    { a: "Venus", b: "Pluto", aName: "Венера", bName: "Плутон", type: "оппозиция", symbol: "☍", orb: 2.4 },
    { a: "Sun", b: "Moon", aName: "Солнце", bName: "Луна", type: "соединение", symbol: "☌", orb: 2.8 },
    { a: "Venus", b: "Mars", aName: "Венера", bName: "Марс", type: "трин", symbol: "△", orb: 2.8 },
    { a: "Sun", b: "Uranus", aName: "Солнце", bName: "Уран", type: "трин", symbol: "△", orb: 3.0 },
    { a: "Venus", b: "Neptune", aName: "Венера", bName: "Нептун", type: "трин", symbol: "△", orb: 3.0 },
  ],
};

const ZODIAC = [
  { name: "Овен", glyph: "♈" },
  { name: "Телец", glyph: "♉" },
  { name: "Близнецы", glyph: "♊" },
  { name: "Рак", glyph: "♋" },
  { name: "Лев", glyph: "♌" },
  { name: "Дева", glyph: "♍" },
  { name: "Весы", glyph: "♎" },
  { name: "Скорпион", glyph: "♏" },
  { name: "Стрелец", glyph: "♐" },
  { name: "Козерог", glyph: "♑" },
  { name: "Водолей", glyph: "♒" },
  { name: "Рыбы", glyph: "♓" },
];

const CX = 320;
const CY = 320;
const R_OUTER = 292;
const R_INNER = 236;
const R_PLANET = 188;
const R_ASPECT = 150;

function polar(lon, r) {
  // 0° Aries at left (9 o'clock), counter-clockwise — classic chart
  const rad = ((180 - lon) * Math.PI) / 180;
  return { x: CX + r * Math.cos(rad), y: CY - r * Math.sin(rad) };
}

function drawZodiac() {
  const g = document.getElementById("zodiac-ring");
  const ns = "http://www.w3.org/2000/svg";

  const outer = document.createElementNS(ns, "circle");
  outer.setAttribute("cx", CX);
  outer.setAttribute("cy", CY);
  outer.setAttribute("r", R_OUTER);
  outer.setAttribute("fill", "none");
  outer.setAttribute("stroke", "rgba(212,168,83,0.45)");
  outer.setAttribute("stroke-width", "1.2");
  g.appendChild(outer);

  const inner = document.createElementNS(ns, "circle");
  inner.setAttribute("cx", CX);
  inner.setAttribute("cy", CY);
  inner.setAttribute("r", R_INNER);
  inner.setAttribute("fill", "none");
  inner.setAttribute("stroke", "rgba(212,168,83,0.28)");
  inner.setAttribute("stroke-width", "1");
  g.appendChild(inner);

  for (let i = 0; i < 12; i++) {
    const lon = i * 30;
    const a = polar(lon, R_INNER);
    const b = polar(lon, R_OUTER);
    const line = document.createElementNS(ns, "line");
    line.setAttribute("x1", a.x);
    line.setAttribute("y1", a.y);
    line.setAttribute("x2", b.x);
    line.setAttribute("y2", b.y);
    line.setAttribute("stroke", "rgba(212,168,83,0.3)");
    line.setAttribute("stroke-width", "1");
    g.appendChild(line);

    const mid = polar(lon + 15, (R_INNER + R_OUTER) / 2);
    const label = document.createElementNS(ns, "text");
    label.setAttribute("x", mid.x);
    label.setAttribute("y", mid.y + 5);
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("class", "zodiac-label");
    label.textContent = ZODIAC[i].glyph;
    g.appendChild(label);
  }
}

function drawAspects() {
  const g = document.getElementById("aspects-layer");
  const ns = "http://www.w3.org/2000/svg";
  const byId = Object.fromEntries(CHART.planets.map((p) => [p.id, p]));
  const colors = {
    соединение: "rgba(224,122,79,0.75)",
    оппозиция: "rgba(196,120,74,0.7)",
    квадрат: "rgba(196,90,70,0.65)",
    трин: "rgba(143,191,122,0.7)",
    секстиль: "rgba(126,184,201,0.65)",
  };

  CHART.aspects.forEach((asp, i) => {
    const pa = byId[asp.a];
    const pb = byId[asp.b];
    if (!pa || !pb) return;
    const a = polar(pa.lon, R_ASPECT);
    const b = polar(pb.lon, R_ASPECT);
    const line = document.createElementNS(ns, "line");
    line.setAttribute("x1", a.x);
    line.setAttribute("y1", a.y);
    line.setAttribute("x2", b.x);
    line.setAttribute("y2", b.y);
    line.setAttribute("stroke", colors[asp.type] || "rgba(212,168,83,0.4)");
    line.setAttribute("stroke-width", asp.orb < 1.5 ? "1.6" : "1");
    line.setAttribute("class", "aspect-line");
    line.style.animationDelay = `${0.4 + i * 0.06}s`;
    g.appendChild(line);
  });
}

function staggerPlanets(planets) {
  const sorted = [...planets].sort((a, b) => a.lon - b.lon);
  const placed = [];
  const minGap = 7;
  for (const p of sorted) {
    let lon = p.lon;
    if (placed.length) {
      const prev = placed[placed.length - 1];
      let gap = lon - prev.displayLon;
      if (gap < 0) gap += 360;
      if (gap < minGap) lon = prev.displayLon + minGap;
    }
    placed.push({ ...p, displayLon: lon % 360 });
  }
  return placed;
}

function drawPlanets() {
  const g = document.getElementById("planets-layer");
  const ns = "http://www.w3.org/2000/svg";
  const visible = CHART.planets.filter((p) => p.id !== "Chiron");
  const placed = staggerPlanets(visible);

  placed.forEach((p, i) => {
    const tick = polar(p.lon, R_INNER - 4);
    const tip = polar(p.lon, R_INNER - 14);
    const mark = document.createElementNS(ns, "line");
    mark.setAttribute("x1", tick.x);
    mark.setAttribute("y1", tick.y);
    mark.setAttribute("x2", tip.x);
    mark.setAttribute("y2", tip.y);
    mark.setAttribute("stroke", "rgba(212,168,83,0.55)");
    mark.setAttribute("stroke-width", "1.5");
    g.appendChild(mark);

    const pos = polar(p.displayLon, R_PLANET);
    const text = document.createElementNS(ns, "text");
    text.setAttribute("x", pos.x);
    text.setAttribute("y", pos.y + 6);
    text.setAttribute("text-anchor", "middle");
    text.setAttribute("class", `planet-glyph ${p.element}`);
    text.setAttribute("filter", "url(#softGlow)");
    text.style.opacity = "0";
    text.style.animation = `rise 0.5s ${0.5 + i * 0.05}s ease forwards`;
    text.textContent = p.glyph + (p.retro ? "ʳ" : "");
    text.setAttribute("data-name", p.name);
    g.appendChild(text);
  });
}

function renderPlanetGrid() {
  const grid = document.getElementById("planet-grid");
  CHART.planets.forEach((p, i) => {
    const el = document.createElement("div");
    el.className = "planet-card";
    el.style.animationDelay = `${0.05 * i}s`;
    el.innerHTML = `
      <span class="glyph" aria-hidden="true">${p.glyph}</span>
      <span class="name">${p.name}${p.retro ? ' <span class="retro">R</span>' : ""}</span>
      <span class="pos">${p.text} ${p.sign}</span>
    `;
    grid.appendChild(el);
  });
}

function renderAspects() {
  const list = document.getElementById("aspect-list");
  CHART.aspects.forEach((a, i) => {
    const li = document.createElement("li");
    li.style.animationDelay = `${0.04 * i}s`;
    li.innerHTML = `
      <span class="sym">${a.symbol}</span>
      <div class="pair">${a.aName} ${a.type} ${a.bName}<br /><span>${a.aName} ${a.symbol} ${a.bName}</span></div>
      <span class="orb">орб ${a.orb}°</span>
    `;
    list.appendChild(li);
  });
}

function initStars() {
  const canvas = document.getElementById("stars");
  const ctx = canvas.getContext("2d");
  let w, h, stars;

  function resize() {
    w = canvas.width = window.innerWidth;
    h = canvas.height = window.innerHeight;
    stars = Array.from({ length: Math.min(160, Math.floor((w * h) / 9000)) }, () => ({
      x: Math.random() * w,
      y: Math.random() * h,
      r: Math.random() * 1.4 + 0.2,
      a: Math.random(),
      s: 0.002 + Math.random() * 0.008,
    }));
  }

  function frame() {
    ctx.clearRect(0, 0, w, h);
    for (const s of stars) {
      s.a += s.s;
      const alpha = 0.25 + Math.abs(Math.sin(s.a * Math.PI * 2)) * 0.75;
      ctx.beginPath();
      ctx.fillStyle = `rgba(232, 239, 233, ${alpha * 0.7})`;
      ctx.arc(s.x, s.y, s.r, 0, Math.PI * 2);
      ctx.fill();
    }
    requestAnimationFrame(frame);
  }

  resize();
  window.addEventListener("resize", resize);
  frame();
}

document.addEventListener("DOMContentLoaded", () => {
  drawZodiac();
  drawAspects();
  drawPlanets();
  renderPlanetGrid();
  renderAspects();
  initStars();
});
