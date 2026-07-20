#!/usr/bin/env node
/** Расчёт натальной карты · 09.04.1986 · 12:00 UT */
const fs = require("fs");
const path = require("path");
const Astronomy = require("astronomy-engine");

const signs = [
  "Овен", "Телец", "Близнецы", "Рак", "Лев", "Дева",
  "Весы", "Скорпион", "Стрелец", "Козерог", "Водолей", "Рыбы",
];
const glyphs = {
  Sun: "☉", Moon: "☽", Mercury: "☿", Venus: "♀", Mars: "♂",
  Jupiter: "♃", Saturn: "♄", Uranus: "♅", Neptune: "♆", Pluto: "♇",
};
const namesRu = {
  Sun: "Солнце", Moon: "Луна", Mercury: "Меркурий", Venus: "Венера", Mars: "Марс",
  Jupiter: "Юпитер", Saturn: "Сатурн", Uranus: "Уран", Neptune: "Нептун", Pluto: "Плутон",
};

function lonInfo(lon) {
  lon = ((lon % 360) + 360) % 360;
  const i = Math.floor(lon / 30);
  const d = lon - i * 30;
  let deg = Math.floor(d);
  let min = Math.round((d - deg) * 60);
  if (min === 60) {
    min = 0;
    deg += 1;
  }
  return {
    lon: +lon.toFixed(4),
    signIndex: i,
    sign: signs[i],
    deg,
    min,
    text: `${deg}°${String(min).padStart(2, "0")}′`,
  };
}

function eclLon(body, date) {
  return Astronomy.Ecliptic(Astronomy.GeoVector(body, date, true)).elon;
}

function isRetro(body, date) {
  if (body === "Sun" || body === "Moon") return false;
  const t1 = new Date(date.getTime() - 12 * 3600 * 1000);
  const t2 = new Date(date.getTime() + 12 * 3600 * 1000);
  const da = ((eclLon(body, t2) - eclLon(body, t1) + 540) % 360) - 180;
  return da < 0;
}

const date = new Date(Date.UTC(1986, 3, 9, 12, 0, 0));
const bodies = ["Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"];

const planets = bodies.map((b) => {
  const info = lonInfo(eclLon(b, date));
  return {
    id: b,
    name: namesRu[b],
    glyph: glyphs[b],
    ...info,
    retro: isRetro(b, date),
  };
});

planets.push({
  id: "Node",
  name: "Сев. узел",
  glyph: "☊",
  lon: 0.6167,
  signIndex: 0,
  sign: "Овен",
  deg: 0,
  min: 37,
  text: "0°37′",
  retro: true,
});
planets.push({
  id: "Chiron",
  name: "Хирон",
  glyph: "⚷",
  lon: 70.85,
  signIndex: 2,
  sign: "Близнецы",
  deg: 10,
  min: 51,
  text: "10°51′",
  retro: false,
});

const aspectDefs = [
  { name: "соединение", symbol: "☌", angle: 0, orb: 8 },
  { name: "секстиль", symbol: "⚹", angle: 60, orb: 5 },
  { name: "квадрат", symbol: "□", angle: 90, orb: 7 },
  { name: "трин", symbol: "△", angle: 120, orb: 7 },
  { name: "оппозиция", symbol: "☍", angle: 180, orb: 7 },
];

const main = planets.filter((p) => !["Node", "Chiron"].includes(p.id));
const aspects = [];
for (let i = 0; i < main.length; i++) {
  for (let j = i + 1; j < main.length; j++) {
    let d = Math.abs(main[i].lon - main[j].lon);
    if (d > 180) d = 360 - d;
    for (const a of aspectDefs) {
      const orb = Math.abs(d - a.angle);
      if (orb <= a.orb) {
        aspects.push({
          a: main[i].id,
          b: main[j].id,
          aName: main[i].name,
          bName: main[j].name,
          type: a.name,
          symbol: a.symbol,
          orb: +orb.toFixed(1),
        });
      }
    }
  }
}
aspects.sort((x, y) => x.orb - y.orb);

const out = {
  birth: {
    date: "09.04.1986",
    dateIso: "1986-04-09",
    time: "12:00 UT (время неизвестно)",
    place: "место неизвестно",
    note: "Позиции планет на полдень UT. 09.04.1986 — новолуние и частичное солнечное затмение в Овне.",
  },
  planets,
  aspects,
};

const root = path.join(__dirname, "..");
fs.writeFileSync(path.join(root, "natal-data.json"), JSON.stringify(out, null, 2));

console.log("NOVA · натальная карта 09.04.1986\n");
for (const p of planets) {
  console.log(`${p.glyph} ${p.name.padEnd(12)} ${p.text} ${p.sign}${p.retro ? " R" : ""}`);
}
console.log("\nАспекты:");
for (const a of aspects.slice(0, 12)) {
  console.log(`  ${a.symbol} ${a.aName} ${a.type} ${a.bName} (${a.orb}°)`);
}
console.log("\n✓ natal-data.json обновлён");
