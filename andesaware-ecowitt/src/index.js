export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // --- CORS (apex + www)
    const ALLOW = new Set(["https://andesaware.com", "https://www.andesaware.com"]);
    const origin = request.headers.get("origin");
    const allowOrigin = ALLOW.has(origin) ? origin : "https://andesaware.com";
    const cors = {
      "Access-Control-Allow-Origin": allowOrigin,
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Vary": "Origin",
      "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    // --- landing
    if (path === "/") {
      return new Response("andesaware api ready", {
        headers: { "content-type": "text/plain; charset=utf-8", ...cors }
      });
    }

    // --- debug echo (see exactly what the device sends)
    if (path === "/api/echo" || path.startsWith("/api/echo/")) {
      let parsed = null;
      const ct = (request.headers.get("content-type") || "").toLowerCase();
      if (ct.includes("application/json")) parsed = await request.json().catch(() => null);
      else if (ct.includes("application/x-www-form-urlencoded")) {
        const body = new URLSearchParams(await request.text());
        parsed = {};
        body.forEach((v, k) => (parsed[k] = v));
      }
      const out = {
        method: request.method,
        url: request.url,
        path,
        headers: Object.fromEntries([...request.headers.entries()].slice(0, 200)),
        query: Object.fromEntries(url.searchParams.entries()),
        parsed
      };
      return new Response(JSON.stringify(out, null, 2), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // --- save raw sample
    const save = async (data) => {
      const ts = Date.now();
      await env.DB.prepare("INSERT INTO samples (ts, payload) VALUES (?, ?)")
        .bind(ts, JSON.stringify(data))
        .run();
      return { ts, data };
    };


    // --- receiver (Ecowitt + Weather Underground compatible)
    if (path === "/api/ecowitt" || path.startsWith("/api/ecowitt/")) {
      let params = {};
      if (request.method === "GET") {
        url.searchParams.forEach((v, k) => (params[k] = v));
      } else if (request.method === "POST") {
        const ct = (request.headers.get("content-type") || "").toLowerCase();
        if (ct.includes("application/json")) params = await request.json().catch(() => ({}));
        else if (ct.includes("application/x-www-form-urlencoded")) {
          const body = new URLSearchParams(await request.text());
          body.forEach((v, k) => (params[k] = v));
        } else {
          return new Response("unsupported content-type", { status: 415, headers: cors });
        }
      } else {
        return new Response("method not allowed", { status: 405, headers: cors });
      }

      // --- only enforce passkey if client sends it (Ecowitt devices)
      if ("passkey" in params) {
        if (env.ECOWITT_PASSKEY && params.passkey !== env.ECOWITT_PASSKEY) {
          return new Response("bad passkey", { status: 401, headers: cors });
        }
      }

      // save raw params
      await save(params);
      return new Response("OK", { headers: cors });
    }
        

    // --- latest (SI)
    if (path === "/api/latest") {
      const row = await env.DB
        .prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1")
        .first();

      const out = row
        ? { ts: row.ts, ts_local: fmt(row.ts), data: toSI(JSON.parse(row.payload || "{}")) }
        : {};

      return new Response(JSON.stringify(out), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // --- /api/health : quick liveness + last sample time
    if (path === "/api/health") {
      const row = await env.DB
        .prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1")
        .first();

      const now = Date.now();
      let status = "no-data";
      let lag_s = null;
      let last = null;

      if (row) {
        lag_s = Math.round((now - row.ts) / 1000);
        status = lag_s <= 120 ? "ok" : "stale"; // <=2 minutes considered healthy
        try { last = JSON.parse(row.payload || "{}"); } catch { last = null; }
      }

      return new Response(JSON.stringify({
        status,
        now,
        now_local: fmt(now),
        last_ts: row?.ts ?? null,
        last_ts_local: row ? fmt(row.ts) : null,
        lag_s,
        last
      }), {
        headers: {
          "content-type": "application/json",
          "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
          ...cors
        }
      });
    }

    // --- latest raw (exact match so it isn't shadowed)
    if (path === "/api/latest_raw") {
      const row = await env.DB
        .prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1")
        .first();

      const out = row
        ? { ts: row.ts, ts_local: fmt(row.ts), payload: JSON.parse(row.payload || "{}") }
        : {};

      return new Response(JSON.stringify(out), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // --- history (SI)  /api/history?hours=24
    if (path === "/api/history") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;

      const rows = await env.DB
        .prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC")
        .bind(since)
        .all();

      const series = rows.results.map((r) => {
        const raw = JSON.parse(r.payload || "{}");
        return { t: r.ts, t_local: fmt(r.ts), ...toSI(raw) };
      });

      return new Response(JSON.stringify(series), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // --- history raw (exact match)
    if (path === "/api/history_raw") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;

      const rows = await env.DB
        .prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC")
        .bind(since)
        .all();

      const out = rows.results.map((r) => ({
        ts: r.ts,
        ts_local: fmt(r.ts),
        payload: JSON.parse(r.payload || "{}"),
      }));

      return new Response(JSON.stringify(out), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // --- simple ingest helper (manual tests)
    if (path === "/api/ingest") {
      let params = {};
      if (request.method === "GET") {
        new URL(request.url).searchParams.forEach((v, k) => (params[k] = v));
      } else if (request.method === "POST") {
        const ct = (request.headers.get("content-type") || "").toLowerCase();
        if (ct.includes("application/json")) params = await request.json().catch(() => ({}));
        else if (ct.includes("application/x-www-form-urlencoded")) {
          const body = new URLSearchParams(await request.text());
          body.forEach((v, k) => (params[k] = v));
        }
      }
      await env.DB.prepare("INSERT INTO samples (ts, payload) VALUES (?, ?)")
        .bind(Date.now(), JSON.stringify(params)).run();
      return new Response(JSON.stringify({ ok: true, saved: true, params }), {
        headers: { "content-type": "application/json", "Cache-Control": "no-store", ...cors }
      });
    }

    // --- CSV export (all rows, or window with ?hours=24)
    if (path === "/api/export.csv") {
      const hoursParam = url.searchParams.get("hours");
      const rows = hoursParam
        ? await env.DB
            .prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC")
            .bind(Date.now() - Number(hoursParam) * 3600 * 1000)
            .all()
        : await env.DB
            .prepare("SELECT ts, payload FROM samples ORDER BY ts ASC")
            .all();

      const samples = rows.results.map((r) => {
        const raw = JSON.parse(r.payload || "{}");
        const si = toSI(raw);
        return {
          ts: r.ts,
          ts_local: fmt(r.ts),
          ...si,            // SI first for convenience
          ...raw,           // raw fields also included
          raw_json: JSON.stringify(raw),
        };
      });

      // union of keys across all samples
      const keys = Array.from(
        samples.reduce((s, o) => { Object.keys(o).forEach((k) => s.add(k)); return s; },
                       new Set(["ts", "ts_local"]))
      );

      const esc = (v) =>
        v == null ? "" : String(v).replace(/"/g, '""').replace(/,/g, ".");
      const csv = [keys.join(","), ...samples.map((s) => keys.map((k) => `"${esc(s[k])}"`).join(","))].join("\n");

      return new Response(csv, {
        headers: {
          "content-type": "text/csv; charset=utf-8",
          "content-disposition": `attachment; filename="ecowitt_full.csv"`,
          ...cors,
        },
      });
    }

    return new Response("not found", { status: 404, headers: cors });
  },

  // cron job (you can keep this hourly in wrangler.toml with a CRON trigger)
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(appendToGitHubCSV(env).catch((e) => console.error("archive error", e)));
  },
};

/* ---------- helpers (shared by fetch & scheduled) ---------- */

// DD/MM/YYYY HH:mm (UTC)
function fmt(ms) {
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(d.getUTCDate())}/${pad(d.getUTCMonth() + 1)}/${d.getUTCFullYear()} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

// robust numeric helpers + unit conversions
const num = (v) => (v == null || v === "" ? null : Number(v));
const pick = (o, ...keys) => { for (const k of keys) if (o[k] != null) return o[k]; return null; };

// Magnus dew point (°C)
function dewpointC(tC, rh) {
  if (tC == null || rh == null) return null;
  const a = 17.62, b = 243.12;
  const gamma = (a * tC) / (b + tC) + Math.log(rh / 100);
  return (b * gamma) / (a - gamma);
}

// Ecowitt + Weather Underground → SI (C, m/s, hPa, mm)
function toSI(d) {
  const norm = (obj) => Object.fromEntries(
    Object.entries(obj).map(([k, v]) => [k.toLowerCase(), v])
  );
  const u = norm(d); // normalized lowercase copy

  const num = (v) => (v == null || v === "" ? null : Number(v));
  const pick = (o, ...keys) => { for (const k of keys) if (o[k] != null) return o[k]; return null; };

  // --- temperature (°F → °C)
  const tOutF = num(pick(u, "tempf", "outtempf", "temperature")); 
  const tOutC = tOutF != null ? (tOutF - 32) * 5/9 : null;

  const rhOut = num(pick(u, "humidity", "outhumidity"));

  const feelsOutF = num(pick(u, "feelslikef", "heatindexf", "windchillf"));
  const feelsOutC = feelsOutF != null ? (feelsOutF - 32) * 5/9 : null;

  const dewOutF = num(pick(u, "dewpointf"));
  const dewpointC = (t, rh) => {
    if (t == null || rh == null) return null;
    const a = 17.62, b = 243.12;
    const g = (a * t) / (b + t) + Math.log(rh / 100);
    return (b * g) / (a - g);
  };
  const dewOutC = dewOutF != null ? (dewOutF - 32) * 5/9 : dewpointC(tOutC, rhOut);

  // --- indoor (same logic)
  const tInF = num(pick(u, "indoortempf", "tempinf"));
  const tInC = tInF != null ? (tInF - 32) * 5/9 : null;
  const rhIn = num(pick(u, "indoorhumidity", "humidityin"));

  // --- solar / uv
  const solar = num(pick(u, "solarradiation", "solar"));
  const uv = num(pick(u, "uv"));

  // --- rain (in → mm)
  const in2mm = (x) => (x == null ? null : Number(x) * 25.4);
  const rainRate   = in2mm(pick(u, "rainratein"));   // Ecowitt
  const rainWU     = in2mm(pick(u, "rainin"));       // WU incremental
  const rainHourly = in2mm(pick(u, "hourlyrainin"));
  const rainDaily  = in2mm(pick(u, "dailyrainin"));
  const rainEvent  = in2mm(pick(u, "eventrainin"));
  const rain24h    = in2mm(pick(u, "24hourrainin", "rain24hin"));

  // if WU only sends incremental rain, estimate mm/hr
  const rainRateFinal = rainRate != null ? rainRate :
                        rainWU != null ? rainWU * 60 : null;

  // --- wind (mph → m/s)
  const mph2ms = (x) => (x == null ? null : Number(x) * 0.44704);
  const wind = mph2ms(pick(u, "windspeedmph"));
  const gust = mph2ms(pick(u, "windgustmph"));
  const dir  = num(pick(u, "winddir"));
  const dir10m = num(pick(u, "winddir_avg10m", "windavgdir", "winddir10m"));

  // --- pressure (inHg → hPa)
  const inHg2hPa = (x) => (x == null ? null : Number(x) * 33.8639);
  const presRel  = num(pick(u, "baromrelhpa"));
  const presAbs  = num(pick(u, "baromabshpa"));
  const presWU   = num(pick(u, "baromin"));        // WU
  const presRelHpa = presRel != null ? presRel : inHg2hPa(presWU);
  const presAbsHpa = presAbs != null ? presAbs : null;

  // --- metadata
  const stationID = pick(u, "id", "station");  // WU ID
  const stationType = pick(u, "stationtype", "softwaretype");

  return {
    // outdoor
    outdoor_temp_c: tOutC,
    outdoor_feels_like_c: feelsOutC,
    outdoor_dewpoint_c: dewOutC,
    outdoor_humidity_pct: rhOut,

    // indoor
    indoor_temp_c: tInC,
    indoor_humidity_pct: rhIn,

    // solar & uv
    solar_wm2: solar,
    uv_index: uv,

    // rain
    rain_rate_mm_hr: rainRateFinal,
    rain_hourly_mm:  rainHourly,
    rain_daily_mm:   rainDaily,
    rain_event_mm:   rainEvent,
    rain_24h_mm:     rain24h,

    // wind
    wind_speed_ms: wind,
    wind_gust_ms:  gust,
    wind_dir_deg:  dir,
    wind_dir_avg10m_deg: dir10m,

    // pressure
    pressure_rel_hpa: presRelHpa,
    pressure_abs_hpa: presAbsHpa,

    // metadata
    station_id: stationID ?? null,
    stationtype: stationType ?? null,
  };
}

// Append rows from last N minutes to a GitHub CSV
async function appendToGitHubCSV(env, minutes = 60) {
  const since = Date.now() - minutes * 60 * 1000;

  const rows = await env.DB
    .prepare("SELECT ts, payload FROM samples WHERE ts >= ? ORDER BY ts ASC")
    .bind(since)
    .all();

  if (!rows.results.length) {
    return { ok: true, message: "no data in window", path: null };
  }

  const samples = rows.results.map((r) => {
    const raw = JSON.parse(r.payload || "{}");
    const si = toSI(raw);
    return {
      ts: r.ts,
      ts_local: fmt(r.ts),
      ...si,
      ...raw,
      raw_json: JSON.stringify(raw),
    };
  });

  // union keys (stable header)
  const keys = Array.from(
    samples.reduce((s, o) => { Object.keys(o).forEach((k) => s.add(k)); return s; },
                   new Set(["ts","ts_local"]))
  );

  const esc = (v) => (v == null ? "" : String(v).replace(/"/g, '""').replace(/,/g, "."));
  const newChunk = samples.map((s) => keys.map((k) => `"${esc(s[k])}"`).join(",")).join("\n") + "\n";

  const repo     = env.GH_REPO;           // "owner/repo"
  const branch   = env.GH_BRANCH || "main";
  const filepath = "archives/ecowitt/ecowitt_history.csv";
  const api = `https://api.github.com/repos/${repo}/contents/${encodeURIComponent(filepath)}`;
  const headers = {
    "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
    "Accept": "application/vnd.github+json",
    "User-Agent": "andesaware-ecowitt-archiver",
  };

  // get + append
  const get = await fetch(`${api}?ref=${branch}`, { headers });
  let sha, oldContent = "";
  if (get.status === 200) {
    const json = await get.json();
    sha = json.sha;
    oldContent = atob(json.content.replace(/\n/g, ""));
  }

  const contentB64 = btoa(unescape(encodeURIComponent(oldContent + newChunk)));
  const body = { message: `append ${minutes}min`, content: contentB64, branch, ...(sha ? { sha } : {}) };

  const put = await fetch(api, { method: "PUT", headers, body: JSON.stringify(body) });
  if (!put.ok) {
    const txt = await put.text();
    return { ok: false, message: `github put failed ${put.status}: ${txt}`, path: filepath };
  }
  return { ok: true, message: "appended", path: filepath };
}
