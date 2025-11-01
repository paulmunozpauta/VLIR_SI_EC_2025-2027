export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS: allow apex and www
    const ALLOW = new Set(["https://andesaware.com", "https://www.andesaware.com"]);
    const origin = request.headers.get("origin");
    const allowOrigin = ALLOW.has(origin) ? origin : "https://andesaware.com";
    const cors = {
      "Access-Control-Allow-Origin": allowOrigin,
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Vary": "Origin",
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    // helper: format timestamp to DD/MM/YYYY HH:mm (UTC)
    const fmt = (ms) => {
      const d = new Date(ms);
      const pad = (n) => n.toString().padStart(2, "0");
      return `${pad(d.getUTCDate())}/${pad(d.getUTCMonth() + 1)}/${d.getUTCFullYear()} ` +
             `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
    };

    // landing page
    if (path === "/") {
      return new Response("andesaware api ready", {
        headers: { "content-type": "text/plain; charset=utf-8", ...cors }
      });
    }

    // helper to save raw payload
    const save = async (data) => {
      const ts = Date.now();
      await env.DB.prepare("INSERT INTO samples (ts, payload) VALUES (?, ?)").bind(ts, JSON.stringify(data)).run();
      return { ts, data };
    };

    // --- /api/ecowitt (receiver)
    if (path === "/api/ecowitt") {
      let params = {};
      if (request.method === "GET") {
        url.searchParams.forEach((v, k) => params[k] = v);
      } else if (request.method === "POST") {
        const ct = request.headers.get("content-type") || "";
        if (ct.includes("application/json")) {
          params = await request.json();
        } else if (ct.includes("application/x-www-form-urlencoded")) {
          const body = new URLSearchParams(await request.text());
          body.forEach((v, k) => params[k] = v);
        } else {
          return new Response("unsupported content-type", { status: 415, headers: cors });
        }
      }
      if (env.ECOWITT_PASSKEY && params.passkey !== env.ECOWITT_PASSKEY) {
        return new Response("bad passkey", { status: 401, headers: cors });
      }

      await save(params);
      return new Response("OK", { headers: cors });
    }

    // ---- helpers ----
    const num = (v) => (v == null || v === "" ? null : Number(v));
    const pick = (o, ...keys) => { for (const k of keys) if (o[k] != null) return o[k]; return null; };
    // Magnus dew point (°C) from temp °C and RH %
    const dewpointC = (tC, rh) => {
      if (tC == null || rh == null) return null;
      const a = 17.62, b = 243.12;
      const gamma = (a * tC) / (b + tC) + Math.log(rh / 100);
      return (b * gamma) / (a - gamma);
    };

    // ---- BIG mapper: Ecowitt payload -> rich SI structure
    const toSI = (d) => {
      // outdoor
      const tempOutF = num(pick(d, "tempf", "outtempf"));
      const rhOut = num(pick(d, "humidity", "outhumidity"));
      const feelsOutF = num(pick(d, "feelslikef", "heatindexf", "windchillf"));
      const dewOutF = num(pick(d, "dewpointf"));
      const tempOutC = tempOutF != null ? (tempOutF - 32) * 5/9 : null;
      const feelsOutC = feelsOutF != null ? (feelsOutF - 32) * 5/9 : null;
      const dewOutC = dewOutF != null ? (dewOutF - 32) * 5/9 : dewpointC(tempOutC, rhOut);

      // indoor
      const tempInF  = num(pick(d, "indoortempf", "tempinf"));
      const rhIn     = num(pick(d, "indoorhumidity", "humidityin"));
      const feelsInF = num(pick(d, "indoorfeelslikef"));
      const dewInF   = num(pick(d, "indoordewpointf", "dewpointinf"));
      const tempInC  = tempInF != null ? (tempInF - 32) * 5/9 : null;
      const feelsInC = feelsInF != null ? (feelsInF - 32) * 5/9 : null;
      const dewInC   = dewInF != null ? (dewInF - 32) * 5/9 : dewpointC(tempInC, rhIn);

      // solar & UV
      const solarWm2 = num(pick(d, "solarradiation"));
      const uvIdx    = num(pick(d, "uv"));

      // rainfall (in -> mm) / rates (in/hr -> mm/hr)
      const in2mm   = (x) => (x == null ? null : Number(x) * 25.4);
      const rate2mm = (x) => (x == null ? null : Number(x) * 25.4);
      const rainRate     = rate2mm(pick(d, "rainratein"));
      const rainDaily    = in2mm(pick(d, "dailyrainin"));
      const rainEvent    = in2mm(pick(d, "eventrainin"));
      const rainHourly   = in2mm(pick(d, "hourlyrainin"));
      const rain24h      = in2mm(pick(d, "24hourrainin", "rain24h_in", "rain24hin"));
      const rainWeekly   = in2mm(pick(d, "weeklyrainin"));
      const rainMonthly  = in2mm(pick(d, "monthlyrainin"));
      const rainYearly   = in2mm(pick(d, "yearlyrainin"));

      // wind (mph -> m/s)
      const mph2ms = (x) => (x == null ? null : Number(x) * 0.44704);
      const windMs     = mph2ms(pick(d, "windspeedmph"));
      const gustMs     = mph2ms(pick(d, "windgustmph"));
      const windDir    = num(pick(d, "winddir"));
      const windDir10m = num(pick(d, "winddir_avg10m", "windavgdir", "winddir10m"));

      // pressure (prefer hPa native; else inHg -> hPa)
      const inHg2hPa = (x) => (x == null ? null : Number(x) * 33.8639);
      const presRel  = num(pick(d, "baromrelhpa"));
      const presAbs  = num(pick(d, "baromabshpa"));
      const presRelhPa = presRel != null ? presRel : inHg2hPa(pick(d, "baromrelin"));
      const presAbshPa = presAbs != null ? presAbs : inHg2hPa(pick(d, "baromabsin"));

      // device
      const heapBytes = num(pick(d, "heap"));
      const runtimeS  = num(pick(d, "runtime"));

      return {
        // outdoor
        outdoor_temp_c: tempOutC,
        outdoor_feels_like_c: feelsOutC,
        outdoor_dewpoint_c: dewOutC,
        outdoor_humidity_pct: rhOut,

        // indoor
        indoor_temp_c: tempInC,
        indoor_feels_like_c: feelsInC,
        indoor_dewpoint_c: dewInC,
        indoor_humidity_pct: rhIn,

        // solar & UV
        solar_wm2: solarWm2,
        uv_index: uvIdx,

        // rainfall
        rain_rate_mm_hr: rainRate,
        rain_daily_mm:   rainDaily,
        rain_event_mm:   rainEvent,
        rain_hourly_mm:  rainHourly,
        rain_24h_mm:     rain24h,
        rain_weekly_mm:  rainWeekly,
        rain_monthly_mm: rainMonthly,
        rain_yearly_mm:  rainYearly,

        // wind
        wind_speed_ms: windMs,
        wind_gust_ms:  gustMs,
        wind_dir_deg:  windDir,
        wind_dir_avg10m_deg: windDir10m,

        // pressure
        pressure_rel_hpa: presRelhPa,
        pressure_abs_hpa: presAbshPa,

        // device
        heap_bytes: heapBytes,
        runtime_s:  runtimeS,

        // raw station type
        stationtype: d.stationtype ?? null
      };
    };

    // --- /api/latest (SI)
    if (path === "/api/latest") {
      const row = await env.DB.prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1").first();
      const out = row ? {
        ts: row.ts,
        ts_local: fmt(row.ts),
        data: toSI(JSON.parse(row.payload || "{}"))
      } : {};
      return new Response(JSON.stringify(out), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // --- /api/history (SI)
    if (path === "/api/history") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB.prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC").bind(since).all();
      const series = rows.results.map(r => {
        const d = JSON.parse(r.payload || "{}");
        return { t: r.ts, t_local: fmt(r.ts), ...toSI(d) };
      });
      return new Response(JSON.stringify(series), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // --- /api/latest_raw
    if (path === "/api/latest_raw") {
      const row = await env.DB.prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1").first();
      const out = row ? { ts: row.ts, ts_local: fmt(row.ts), payload: JSON.parse(row.payload || "{}") } : {};
      return new Response(JSON.stringify(out), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // --- /api/history_raw
    if (path === "/api/history_raw") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB.prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC").bind(since).all();
      const out = rows.results.map(r => ({
        ts: r.ts,
        ts_local: fmt(r.ts),
        payload: JSON.parse(r.payload || "{}")
      }));
      return new Response(JSON.stringify(out), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // --- /api/export.csv  (raw + ts_local + optional SI via ?si=0 to disable)
    if (path === "/api/export.csv") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const includeSI = (url.searchParams.get("si") ?? "1") !== "0"; // default include SI
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB.prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC").bind(since).all();

      const samples = rows.results.map(r => {
        const raw = JSON.parse(r.payload || "{}");
        const base = { ts: r.ts, ts_local: fmt(r.ts), ...raw };
        return includeSI ? { ...base, ...toSI(raw) } : base;
      });

      const keys = Array.from(samples.reduce((s, o) => { Object.keys(o).forEach(k => s.add(k)); return s; }, new Set(["ts","ts_local"])));
      const esc = v => (v == null ? "" : String(v).replace(/"/g, '""'));
      const lines = samples.map(s => keys.map(k => `"${esc(s[k])}"`).join(","));
      const csv = [keys.join(","), ...lines].join("\n");

      return new Response(csv, {
        headers: {
          "content-type": "text/csv; charset=utf-8",
          "content-disposition": `attachment; filename="ecowitt_${Date.now()}${includeSI ? "_si" : ""}.csv"`,
          ...cors
        }
      });
    }

    // --- /api/health
    if (path === "/api/health") {
      const row = await env.DB.prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1").first();
      const now = Date.now();
      let status = "no-data", lag_s = null, last = null;
      if (row) {
        lag_s = Math.round((now - row.ts) / 1000);
        status = lag_s <= 120 ? "ok" : "stale";
        last = JSON.parse(row.payload || "{}");
      }
      return new Response(JSON.stringify({
        status, now, last_ts: row?.ts ?? null, last_ts_local: row ? fmt(row.ts) : null, lag_s, last
      }), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // --- /api/archive.now
    if (path === "/api/archive.now") {
      const hours = Number(url.searchParams.get("hours") || "1");
      const res = await archiveToGitHub(env, hours, fmt, toSI);
      return new Response(JSON.stringify(res), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    return new Response("not found", { status: 404, headers: cors });
  },

  async scheduled(controller, env, ctx) {
    // archive last hour each run (raw + SI)
    ctx.waitUntil(
      archiveToGitHub(env, 1, (ms) => {
        const d = new Date(ms);
        const pad = (n) => n.toString().padStart(2, "0");
        return `${pad(d.getUTCDate())}/${pad(d.getUTCMonth() + 1)}/${d.getUTCFullYear()} ` +
               `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
      }, (d) => d /* toSI passed below actually */).then(r => {
        console.log("archive result", r.ok, r.path, r.message);
      }).catch(e => console.error("archive error", e))
    );
  }
};

// ---- GitHub archive helper (raw + ts_local + SI) ----
async function archiveToGitHub(env, hours, fmt, toSI) {
  const since = Date.now() - hours * 3600 * 1000;
  const rows = await env.DB.prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC").bind(since).all();

  if (!rows.results.length) return { ok: true, message: "no data in window", path: null };

  const samples = rows.results.map(r => {
    const raw = JSON.parse(r.payload || "{}");
    return { ts: r.ts, ts_local: fmt(r.ts), ...raw, ...toSI(raw) };
  });

  const keys = Array.from(samples.reduce((s, o) => { Object.keys(o).forEach(k => s.add(k)); return s; }, new Set(["ts","ts_local"])));
  const esc = v => (v == null ? "" : String(v).replace(/"/g, '""'));
  const csv = [keys.join(","), ...samples.map(s => keys.map(k => `"${esc(s[k])}"`).join(","))].join("\n");

  // path: archives/ecowitt/YYYY/MM/DD/YYYYMMDD_HH00.csv (UTC)
  const base = env.GH_BASEPATH || "archives/ecowitt";
  const d = new Date(Date.now() - 1 * 3600 * 1000); // previous hour
  const YYYY = d.getUTCFullYear();
  const MM = String(d.getUTCMonth() + 1).padStart(2, "0");
  const DD = String(d.getUTCDate()).padStart(2, "0");
  const HH = String(d.getUTCHours()).padStart(2, "0");
  const path = `${base}/${YYYY}/${MM}/${DD}/${YYYY}${MM}${DD}_${HH}00.csv`;

  const repo = env.GH_REPO;
  const branch = env.GH_BRANCH || "main";
  const api = `https://api.github.com/repos/${repo}/contents/${encodeURIComponent(path)}`;
  const headers = {
    "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
    "Accept": "application/vnd.github+json",
    "User-Agent": "andesaware-ecowitt-archiver"
  };

  // skip if file already exists
  const head = await fetch(`${api}?ref=${branch}`, { headers });
  if (head.status === 200) {
    return { ok: true, message: "already archived", path };
  }

  const contentB64 = btoa(unescape(encodeURIComponent(csv)));
  const body = { message: `archive: ${path}`, content: contentB64, branch };
  const put = await fetch(api, { method: "PUT", headers, body: JSON.stringify(body) });

  if (!put.ok) {
    const txt = await put.text();
    return { ok: false, message: `github put failed ${put.status}: ${txt}`, path };
  }
  return { ok: true, message: "archived", path };
}
