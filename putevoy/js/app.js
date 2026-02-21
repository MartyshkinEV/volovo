"use strict";

/**
 * Volovo Putevoy — фронт
 * ВАЖНО:
 * - API относительный: запросы идут на тот же домен (https://dorvol.ru/api/...)
 * - Никаких <script> внутри JS :)
 */

const API = ""; // важно! оставляем пустым, чтобы работало на https://dorvol.ru

// Пескобаза (как в backend)
const SAND_BASE = { lat: 52.036242, lon: 37.887744, radius_km: 0.02 };

// DOM
const oidSelect = document.getElementById("oidSelect");
const dtFrom = document.getElementById("dtFrom");
const dtTo = document.getElementById("dtTo");
const maxJump = document.getElementById("maxJump");
const maxSpeed = document.getElementById("maxSpeed");
const minTripKmEl = document.getElementById("minTripKm");
const btnLoad = document.getElementById("btnLoad");
const btnExportXlsx = document.getElementById("btnExportXlsx");
const btnPrint = document.getElementById("btnPrint");
const stat = document.getElementById("stat");
const mapInfo = document.getElementById("mapInfo");

// итоги
const totalKmPointsCell = document.getElementById("total_km_points_cell");
const totalKmSumCell = document.getElementById("total_km_sum_cell");
const totalTonsSumCell = document.getElementById("total_tons_sum_cell");

// TTD-итоги
const ttdKmDeliveryCell = document.getElementById("ttd_km_delivery_cell");
const ttd38IdleCell = document.getElementById("ttd_38_idle_cell");

// авто-итоги
const vehicleTonnageCell = document.getElementById("vehicle_tonnage_cell");
const tripsCountCell = document.getElementById("trips_count_cell");
const totalTonsCell = document.getElementById("total_tons_cell");

// строки таблицы
const rows = Array.from(document.querySelectorAll("tr.data-row"));
const routeSelects = Array.from(document.querySelectorAll("select.route"));

// Если хочешь — можно убрать и брать из backend.
// Пока оставляю твой справочник авто.
const VEHICLES = new Map([
  [716, { name: "КАМАЗ 532150 Е015НВ48 (дут)", tonnage: 9 }],
  [719, { name: "КАМАЗ 532150 М435КХ48 (дут)", tonnage: 9 }],
  [182, { name: "Камаз О537КР48 (дут)", tonnage: 14 }],
  [432, { name: "Камаз М968ОН48 (дут)", tonnage: 14 }],
  [717, { name: "КАМАЗ ЭД-405А Е102КХ48 (дут)", tonnage: 9 }],
]);

// Роуты получаем с /api/routes
const ROUTES = new Map(); // name -> {road_width_m, road_length_km, pss_tonnage_t}

// ---------- util ----------
function clearTripsOnMap(){
  tripsLayer.clearLayers();
  mapInfo.textContent = "";
}

function normalizeLatLngsFromTrip(trip){
  // trip.coords: [[lat,lon], ...]
  if (Array.isArray(trip?.coords) && trip.coords.length){
    const ll = trip.coords
      .map(p => Array.isArray(p) ? [Number(p[0]), Number(p[1])] : null)
      .filter(p => p && Number.isFinite(p[0]) && Number.isFinite(p[1]));
    if (ll.length) return ll;
  }

  // trip.points: [{lat,lon}, ...]
  if (Array.isArray(trip?.points) && trip.points.length){
    const ll = trip.points
      .map(p => [Number(p.lat), Number(p.lon)])
      .filter(p => Number.isFinite(p[0]) && Number.isFinite(p[1]));
    if (ll.length) return ll;
  }

  return [];
}

function renderTripsFromApiData(data){
  clearTripsOnMap();

  // 1) GeoJSON FeatureCollection
  if (data?.type === "FeatureCollection" && Array.isArray(data.features)){
    const gj = L.geoJSON(data, {
      // Leaflet по умолчанию ожидает [lat,lon], а GeoJSON даёт [lon,lat]
      coordsToLatLng: (coords) => L.latLng(coords[1], coords[0]),
      style: () => ({ weight: 4 })
    }).addTo(tripsLayer);

    const b = gj.getBounds();
    if (b.isValid()) map.fitBounds(b, { padding: [20, 20] });
    mapInfo.textContent = `GeoJSON: ${data.features.length} шт.`;
    return;
  }

  // 2) trips: [{coords|points,...}]
  const trips = Array.isArray(data?.trips) ? data.trips : (Array.isArray(data) ? data : []);
  if (Array.isArray(trips) && trips.length){
    let total = 0;
    const bounds = [];

    trips.forEach((t, idx) => {
      const ll = normalizeLatLngsFromTrip(t);
      if (ll.length < 2) return;

      const line = L.polyline(ll, { weight: 4 }).addTo(tripsLayer);
      bounds.push(line.getBounds());
      total += 1;
    });

    if (bounds.length){
      // объединяем bounds
      let b = bounds[0];
      for (let i = 1; i < bounds.length; i++) b = b.extend(bounds[i]);
      if (b.isValid()) map.fitBounds(b, { padding: [20, 20] });
    }

    mapInfo.textContent = `Рейсов: ${total}`;
    return;
  }

  // 3) points: [{lat,lon}, ...] или data.points
  const pts = Array.isArray(data?.points) ? data.points : null;
  if (Array.isArray(pts) && pts.length){
    const ll = pts
      .map(p => [Number(p.lat), Number(p.lon)])
      .filter(p => Number.isFinite(p[0]) && Number.isFinite(p[1]));

    if (ll.length >= 2){
      const line = L.polyline(ll, { weight: 4 }).addTo(tripsLayer);
      const b = line.getBounds();
      if (b.isValid()) map.fitBounds(b, { padding: [20, 20] });
      mapInfo.textContent = `Точек: ${ll.length}`;
      return;
    }
  }

  mapInfo.textContent = "Маршруты не найдены (пустой ответ)";
}
async function loadTrips(){
  const oid = String(oidSelect.value || "").trim();
  if (!oid) throw new Error("Выбери авто (OID).");

  const from = dtLocalToApi(dtFrom.value);
  const to = dtLocalToApi(dtTo.value);

  const params = new URLSearchParams({
    oid,
    from,
    to,
    max_jump_km: String(numOr0(maxJump.value) || 1.0),
    max_speed_kmh: String(numOr0(maxSpeed.value) || 180),
    min_trip_km: String(numOr0(minTripKmEl.value) || 1.0),
  });

  // ❗ ВАЖНО: вот тут нужно правильное API.
  // Я ставлю самый логичный вариант. Если у тебя endpoint другой — скажешь, поменяем 1 строку.
  const url = `${API}/api/trips_for_map?${params.toString()}`;

  setStat("Загружаю рейсы с сервера…");
  const data = await fetchJSON(url);
  return data;
}

function pad2(n){ return String(n).padStart(2,"0"); }

function setDefaultDateTimes(){
  const now = new Date();
  const y = now.getFullYear();
  const m = pad2(now.getMonth()+1);
  const d = pad2(now.getDate());
  const date = `${y}-${m}-${d}`;
  if(!dtFrom.value) dtFrom.value = `${date}T00:00`;
  if(!dtTo.value)   dtTo.value   = `${date}T23:59`;
}

function dtLocalToApi(v){
  if(!v) return "";
  return v.replace("T"," ") + ":00";
}

function fmt2(x){
  if(x === null || x === undefined || x === "" || Number.isNaN(Number(x))) return "";
  return Number(x).toFixed(2).replace(".", ",");
}

function numOr0(v){
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

async function fetchJSON(url){
  const r = await fetch(url, { credentials: "same-origin" });
  if(!r.ok){
    const t = await r.text().catch(()=> "");
    throw new Error(`HTTP ${r.status}: ${t.slice(0,200)}`);
  }
  return await r.json();
}

function setStat(text){
  if (stat) stat.textContent = text;
}

function getVehicleTonnage(){
  const oid = Number(oidSelect.value);
  const v = VEHICLES.get(oid);
  return v ? Number(v.tonnage) : 0;
}

function coeffByWidth(widthM){
  const w = Number(widthM);
  if (Math.abs(w - 7) < 0.01) return 1.4;
  if (Math.abs(w - 6) < 0.01) return 1.2;
  return 1.2;
}

function getRowWidth(row){
  const el = row.querySelector(".js-width");
  const w = el ? Number(String(el.textContent).replace(",", ".")) : NaN;
  return Number.isFinite(w) ? w : NaN;
}

function recalcRowKm(row){
  const routeSel  = row.querySelector("select.route");
  const kmCell    = row.querySelector(".trip-km");
  const tonsInput = row.querySelector(".trip-tons");

  if(!kmCell || !tonsInput) return;

  if(!routeSel || !routeSel.value){
    kmCell.textContent = "";
    return;
  }

  const tons = numOr0(tonsInput.value);
  if(tons <= 0){
    kmCell.textContent = "";
    return;
  }

  const widthM = getRowWidth(row);
  const k = coeffByWidth(widthM);
  let km = tons / k;

  const lengthCell = row.querySelector(".js-length");
  const maxLen = Number(String(lengthCell?.textContent ?? "").replace(",", "."));
  if (Number.isFinite(maxLen) && km > maxLen) {
    km = maxLen;
  }

  kmCell.textContent = fmt2(km);
}

function recalcTotals(){
  // сумма тонн
  let tonsSum = 0;
  for (const row of rows){
    const tonsInput = row.querySelector(".trip-tons");
    tonsSum += numOr0(tonsInput?.value);
  }
  totalTonsSumCell.textContent = fmt2(tonsSum);

  // сумма расчётных км
  let kmSum = 0;
  for (const row of rows){
    const kmCell = row.querySelector(".trip-km");
    const n = Number(String(kmCell?.textContent ?? "").replace(",", "."));
    if (Number.isFinite(n)) kmSum += n;
  }
  totalKmSumCell.textContent = fmt2(kmSum);

  // авто-итоги
  const tonnage = getVehicleTonnage();
  vehicleTonnageCell.textContent = tonnage ? fmt2(tonnage) : "—";

  let trips = 0;
  for (const row of rows){
    const routeSel = row.querySelector("select.route");
    if (routeSel && routeSel.value) trips += 1;
  }
  tripsCountCell.textContent = trips ? String(trips) : "—";

  totalTonsCell.textContent = tonsSum ? fmt2(tonsSum) : "—";
}

function fillRouteRowParams(row, routeName){
  const wEl = row.querySelector(".js-width");
  const lenEl = row.querySelector(".js-length");
  const tonsEl = row.querySelector(".js-tons");

  const r = ROUTES.get(routeName);
  if (!r){
    if (wEl) wEl.textContent = "";
    if (lenEl) lenEl.textContent = "";
    if (tonsEl) tonsEl.textContent = "";
    return;
  }

  if (wEl) wEl.textContent = fmt1Safe(r.road_width_m);
  if (lenEl) lenEl.textContent = fmt2Safe(r.road_length_km);
  if (tonsEl) tonsEl.textContent = fmt2Safe(r.pss_tonnage_t);
}

function fmt2Safe(x){
  const n = Number(x);
  return Number.isFinite(n) ? fmt2(n) : "";
}
function fmt1Safe(x){
  const n = Number(x);
  if (!Number.isFinite(n)) return "";
  return n.toFixed(1).replace(".", ",");
}

// ---------- MAP ----------
const map = L.map("map").setView([52.05, 37.99], 11);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: ""
}).addTo(map);
const tripsLayer = L.layerGroup().addTo(map);
// Пескобаза (кружок)
const sandBaseMarker = L.marker([SAND_BASE.lat, SAND_BASE.lon]).addTo(map);
const sandBaseCircle = L.circle([SAND_BASE.lat, SAND_BASE.lon], {
  radius: SAND_BASE.radius_km * 1000
}).addTo(map);

// ---------- init loaders ----------
async function loadOids(){
  setStat("Загружаю список авто (OID)…");
  const data = await fetchJSON(`${API}/api/oids`);

  const oids = Array.isArray(data?.oids) ? data.oids : [];
  oidSelect.innerHTML = "";

  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = "— выбери —";
  oidSelect.appendChild(opt0);

  for (const item of oids){
    const oid = Number(item.oid);
    const cnt = Number(item.points_cnt ?? 0);
    const v = VEHICLES.get(oid);
    const name = v ? v.name : `OID ${oid}`;
    const opt = document.createElement("option");
    opt.value = String(oid);
    opt.textContent = `${name} (OID ${oid}, точек: ${cnt})`;
    oidSelect.appendChild(opt);
  }
}

async function loadRoutes(){
  setStat("Загружаю справочник маршрутов…");
  const data = await fetchJSON(`${API}/api/routes`);

  const routes = Array.isArray(data?.routes) ? data.routes : [];
  ROUTES.clear();

  for (const r of routes){
    if (!r?.name) continue;
    ROUTES.set(r.name, {
      road_width_m: r.road_width_m,
      road_length_km: r.road_length_km,
      pss_tonnage_t: r.pss_tonnage_t
    });
  }

  // заполняем все select.route
  for (const sel of routeSelects){
    sel.innerHTML = "";

    const o0 = document.createElement("option");
    o0.value = "";
    o0.textContent = "— маршрут —";
    sel.appendChild(o0);

    for (const [name] of ROUTES){
      const opt = document.createElement("option");
      opt.value = name;
      opt.textContent = name;
      sel.appendChild(opt);
    }
  }
}

// ---------- events ----------
function attachEvents(){
  // изменение маршрута
  for (const row of rows){
    const routeSel = row.querySelector("select.route");
    const tonsInput = row.querySelector(".trip-tons");

    if (routeSel){
      routeSel.addEventListener("change", () => {
        fillRouteRowParams(row, routeSel.value);
        recalcRowKm(row);
        recalcTotals();
      });
    }

    if (tonsInput){
      tonsInput.addEventListener("input", () => {
        recalcRowKm(row);
        recalcTotals();
      });
    }
  }

  oidSelect.addEventListener("change", () => {
    recalcTotals();
  });

  btnPrint.addEventListener("click", () => window.print());

  btnExportXlsx.addEventListener("click", async () => {
    try{
      setStat("Экспорт… (заглушка)");
      // Тут ты подключишь свой endpoint /api/forms/.../export_xlsx
      // Когда скажешь точный endpoint — впишу.
      setStat("Экспорт пока не подключён в этой версии app.js");
    }catch(e){
      setStat("Ошибка экспорта: " + e.message);
    }
  });

  btnLoad.addEventListener("click", async () => {
  try{
    clearTripsOnMap();

    const data = await loadTrips();
    renderTripsFromApiData(data);

    // если backend отдаёт итоги — можно тут показать в таблице
    // например data.km_total -> totalKmPointsCell.textContent = fmt2(data.km_total)

    setStat("Готово.");
  }catch(e){
    console.error(e);
    setStat("Ошибка загрузки рейсов: " + e.message);
  }
});

}

// ---------- start ----------
(async function main(){
  try{
    setDefaultDateTimes();
    attachEvents();

    await loadRoutes();
    await loadOids();

    setStat("Готово. Выбери авто и даты → «Загрузить».");
  }catch(e){
    console.error(e);
    setStat("Ошибка инициализации: " + e.message);
  }
})();
