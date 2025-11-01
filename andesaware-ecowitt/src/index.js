export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // ----- CORS -----
    const ALLOW = new Set(["https://andesaware.com", "https://www.andesaware.com"]);
    const origin = request.headers.get("origin");
    const allowOrigin = ALLOW.has(origin) ? origin : "https://andesaware.com";
    const cors = {
      "Access-Control-Allow-Origin": allowOrigin,
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Vary": "Origin"
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    // ----- utils -----
    const fmtUTC = (ms) => {
      const d = new Date(ms);
      const pad = (n) => String(n).padStart(2, "0");
      return `${pad(d.getUTCDate())}/${pad(d.getUTCMonth() + 1)}/${d.getUTCFullYear()} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
    };
    const num = (v) => (v == null || v === "" ? null : Number(v));
    const pick = (o, ...keys) => { for (const k of keys) if (o[k] != null) return o[k]; return null; };
    const dewpointC = (tC, rh) => { // Magnus
      if (tC == null || rh == null) return null;
      const a = 17.62, b = 243.12;
      const g = (a * tC) / (b + tC) + Math.log(rh / 100);
      return (b * g) / (a - g);
    };
    const inHg2hPa = (x) => (x == null ? null : Number(x) * 33.8639);
    const mph2ms  = (x) => (x == null ? null : Number(x) * 0.44704);
    const f2c     = (x) => (x == null ? null : (Number(x) - 32) * 5/9);
    const in2mm   = (x) => (x == null ? null : Number(x) * 25.4);

    // ----- landing -----
    if (path === "/") {
      return new Response("andesaware api ready", { headers: { "content-type": "text/plain; charset=utf-8", ...cors } });
    }

    // ----- echo (debug) -----
    if (path === "/api/echo") {
      let parsed = null;
      const ct = request.headers.get("content-type") || "";
      if (request.method === "POST") {
        if (ct.includes("application/json")) parsed = await request.json();
        else if (ct.includes("application/x-www-form-urlencoded")) {
          const body = new URLSearchParams(await request.text());
          parsed = {}; body.forEach((v,k)=>parsed[k]=v);
        }
      }
      const out = {
        method: request.method,
        url: request.url,
        headers: Object.fromEntries([...request.headers].map(([k,v])=>[k.toLowerCase(), v])),
        query: Object.fromEntries(url.searchParams.entries()),
        parsed
      };
      // cache last
      if (env.KV) await env.KV.put("echo-last", JSON.stringify(out), { expirationTtl: 1800 });
      return new Response(JSON.stringify(out, null, 2), { headers: { "content-type": "application/json", ...cors } });
    }
    if (path === "/api/echo-last") {
      const raw = env.KV ? await env.KV.get("echo-last") : null;
      return new Response(raw || "{}", { headers: { "content-type": "application/json", ...cors } });
    }

    // ----- save helper -----
    const save = async (data) => {
      const ts = Date.now();
      await env.DB.prepare("INSERT INTO samples (ts, payload) VALUES (?, ?)").bind(ts, JSON.stringify(data)).run();
      return { ts, data };
    };

    // ===========================================================
    //  RECEIVERS
    // ===========================================================

    // --- Ecowitt receiver (as before)
    if (path === "/api/ecowitt") {
      let params = {};
      if (request.method === "GET") {
        url.searchParams.forEach((v,k)=>params[k]=v);
      } else if (request.method === "POST") {
        const ct = request.headers.get("content-type") || "";
        if (ct.includes("application/json")) params = await request.json();
        else if (ct.includes("application/x-www-form-urlencoded")) {
          const body = new URLSearchParams(await request.text());
          body.forEach((v,k)=>params[k]=v);
        } else {
          return new Response("unsupported content-type", { status: 415, headers: cors });
        }
      } else return new Response("method not allowed", { status: 405, headers: cors });

      if (env.ECOWITT_PASSKEY && params.passkey !== env.ECOWITT_PASSKEY) {
        return new Response("bad passkey", { status: 401, headers: cors });
      }

      params._proto = "ecowitt";
      await save(params);
      return new Response("OK", { headers: cors });
    }

    // --- Wunderground receiver (new)
    // Accepts GET/POST form; username/password are ignored.
    // Common fields: tempf, humidity, windspeedmph, windgustmph, winddir, baromin (inHg),
    // rainin (last hour inches), dailyrainin, dewptf, solarradiation, UV, etc.
    if (path === "/api/wu") {
      let params = {};
      if (request.method === "GET") {
        url.searchParams.forEach((v,k)=>params[k]=v);
      } else if (request.method === "POST") {
        const ct = request.headers.get("content-type") || "";
        if (ct.includes("application/x-www-form-urlencoded")) {
          const body = new URLSearchParams(await request.text());
          body.forEach((v,k)=>params[k]=v);
        } else if (ct.includes("application/json")) {
          params = await request.json();
        } else {
          return new Response("unsupported content-type", { status: 415, headers: cors });
        }
      } else return new Response("method not allowed", { status: 405, headers: cors });

      params._proto = "wu";
      await save(params);
      return new Response("success", { headers: cors });
    }

    // ===========================================================
    //  TRANSFORMS & API OUTPUTS
    // ===========================================================

    // Unified SI transform for Ecowitt or WU payloads
    const toSI = (d) => {
      const proto = d?._proto;

      // outdoor temp & RH (both send tempf/humidity)
      const tempOutC = f2c(pick(d, "tempf", "outtempf"));
      const rhOut    = num(pick(d, "humidity", "outhumidity"));

      // feels/ dew (WU has dewptf; Ecowitt has dewpointf)
      const feelsOutC = f2c(pick(d, "feelslikef", "heatindexf", "windchillf"));
      const dewOutC   = f2c(pick(d, "dewptf", "dewpointf")) ?? dewpointC(tempOutC, rhOut);

      // indoor (WU usually doesn't send)
      const tempInC  = f2c(pick(d, "indoortempf", "tempinf"));
      const rhIn     = num(pick(d, "indoorhumidity", "humidityin"));
      const dewInC   = f2c(pick(d, "indoordewpointf", "dewpointinf")) ?? dewpointC(tempInC, rhIn);
      const feelsInC = f2c(pick(d, "indoorfeelslikef"));

      // solar & uv
      const solarWm2 = num(pick(d, "solarradiation"));
      const uvIndex  = num(pick(d, "uv", "UV"));

      // rainfall:
      // Ecowitt: rainratein (in/hr), dailyrainin, eventrainin, hourlyrainin, 24hourrainin
      // WU: rainin (rain last hour inches), dailyrainin
      const rainRate_mm_hr = in2mm(pick(d, "rainratein")); // only Ecowitt
      const rainHourly_mm  = in2mm(pick(d, "hourlyrainin", "rainin")); // WU uses "rainin" = last hour
      const rainDaily_mm   = in2mm(pick(d, "dailyrainin"));
      const rainEvent_mm   = in2mm(pick(d, "eventrainin"));
      const rain24h_mm     = in2mm(pick(d, "24hourrainin", "rain24h_in", "rain24hin"));
      const rainWeekly_mm  = in2mm(pick(d, "weeklyrainin"));
      const rainMonthly_mm = in2mm(pick(d, "monthlyrainin"));
      const rainYearly_mm  = in2mm(pick(d, "yearlyrainin"));

      // wind
      const wind_ms       = mph2ms(pick(d, "windspeedmph"));
      const wind_gust_ms  = mph2ms(pick(d, "windgustmph"));
      const wind_dir_deg  = num(pick(d, "winddir"));
      const wind_dir_avg10m_deg = num(pick(d, "winddir_avg10m", "windavgdir", "winddir10m"));

      // pressure
      // WU uses baromin (inHg), Ecowitt: baromrelhpa/baromabshpa or inHg variants
      const presRel_hPa = num(pick(d, "baromrelhpa")) ?? inHg2hPa(pick(d, "baromrelin", "baromin"));
      const presAbs_hPa = num(pick(d, "baromabshpa")) ?? inHg2hPa(pick(d, "baromabsin"));

      // device (Ecowitt only)
      const heap_bytes = num(pick(d, "heap"));
      const runtime_s  = num(pick(d, "runtime"));

      return {
        _proto: proto ?? null,
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
        // solar & uv
        solar_wm2: solarWm2,
        uv_index: uvIndex,
        // rain
        rain_rate_mm_hr: rainRate_mm_hr,
        rain_hourly_mm:  rainHourly_mm,
        rain_daily_mm:   rainDaily_mm,
        rain_event_mm:   rainEvent_mm,
        rain_24h_mm:     rain24h_mm,
        rain_weekly_mm:  rainWeekly_mm,
        rain_monthly_mm: rainMonthly_mm,
        rain_yearly_mm:  rainYearly_mm,
        // wind
        wind_speed_ms: wind_ms,
        wind_gust_ms:  wind_gust_ms,
        wind_dir_deg:  wind_dir_deg,
        wind_dir_avg10m_deg: wind_dir_avg10m_deg,
        // pressure
        pressure_rel_hpa: presRel_hPa,
        pressure_abs_hpa: presAbs_hPa,
        // device
        heap_bytes,
        runtime_s,
        // reference
        stationtype: d.stationtype ?? null
      };
    };

    // ----- latest (SI)
    if (path === "/api/latest") {
      const row = await env.DB.prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1").first();
      const out = row ? { ts: row.ts, ts_local: fmtUTC(row.ts), data: toSI(JSON.parse(row.payload || "{}")) } : {};
      return new Response(JSON.stringify(out), { headers: { "content-type": "application/json", ...cors } });
    }

    // ----- history (SI)
    if (path === "/api/history") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB.prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC").bind(since).all();
      const series = rows.results.map(r => ({ t: r.ts, t_local: fmtUTC(r.ts), ...toSI(JSON.parse(r.payload || "{}")) }));
      return new Response(JSON.stringify(series), { headers: { "content-type": "application/json", ...cors } });
    }

    // ----- raw views
    if (path === "/api/latest_raw") {
      const row = await env.DB.prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1").first();
      const out = row ? { ts: row.ts, ts_local: fmtUTC(row.ts), payload: JSON.parse(row.payload || "{}") } : {};
      return new Response(JSON.stringify(out), { headers: { "content-type": "application/json", ...cors } });
    }
    if (path === "/api/history_raw") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB.prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC").bind(since).all();
      const out = rows.results.map(r => ({ ts: r.ts, ts_local: fmtUTC(r.ts), payload: JSON.parse(r.payload || "{}") }));
      return new Response(JSON.stringify(out), { headers: { "content-type": "application/json", ...cors } });
    }

    // ----- export csv (raw + SI)
    if (path === "/api/export.csv") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB.prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC").bind(since).all();
      const samples = rows.results.map(r => {
        const raw = JSON.parse(r.payload || "{}");
        const si  = toSI(raw);
        return { ts: r.ts, ts_local: fmtUTC(r.ts), ...raw, ...Object.fromEntries(Object.entries(si).filter(([k]) => !k.startsWith("_"))) };
      });
      const keys = Array.from(samples.reduce((s, o) => { Object.keys(o).forEach(k => s.add(k)); return s; }, new Set(["ts","ts_local"])));
      const esc = (v) => (v == null ? "" : String(v).replace(/"/g,'""'));
      const csv = [keys.join(","), ...samples.map(s => keys.map(k=>`"${esc(s[k])}"`).join(","))].join("\n");
      return new Response(csv, { headers: { "content-type": "text/csv; charset=utf-8", "content-disposition": `attachment; filename="ecowitt_${Date.now()}.csv"`, ...cors } });
    }

    // ----- health
    if (path === "/api/health") {
      const row = await env.DB.prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1").first();
      const now = Date.now();
      let status = "no-data", lag_s = null, last = null;
      if (row) {
        lag_s = Math.round((now - row.ts) / 1000);
        status = lag_s <= 120 ? "ok" : "stale";
        last = JSON.parse(row.payload || "{}");
      }
      return new Response(JSON.stringify({ status, now, last_ts: row?.ts ?? null, last_ts_local: row ? fmtUTC(row.ts) : null, lag_s, last }), { headers: { "content-type": "application/json", ...cors } });
    }

    // ----- stats (simple)
    if (path === "/api/stats") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB.prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC").bind(since).all();
      const vals = rows.results.map(r => toSI(JSON.parse(r.payload || "{}")));
      const collect = (k) => vals.map(v => v[k]).filter(v => v!=null && !Number.isNaN(v));
      const statsFor = (k) => { const a = collect(k); return a.length ? {count:a.length, min:Math.min(...a), max:Math.max(...a), avg:a.reduce((x,y)=>x+y,0)/a.length} : {count:0,min:null,max:null,avg:null}; };
      const fields = ["outdoor_temp_c","outdoor_humidity_pct","pressure_rel_hpa","wind_speed_ms","rain_hourly_mm","solar_wm2","uv_index"];
      const stats = Object.fromEntries(fields.map(f => [f, statsFor(f)]));
      return new Response(JSON.stringify({ hours, n: rows.results.length, stats }), { headers: { "content-type": "application/json", ...cors } });
    }

    // ----- manual archive
    if (path === "/api/archive.now") {
      const hours = Number(url.searchParams.get("hours") || "1");
      const res = await archiveToGitHub(env, hours, fmtUTC, toSI);
      return new Response(JSON.stringify(res), { headers: { "content-type": "application/json", ...cors } });
    }

    return new Response("not found", { status: 404, headers: cors });
  },

  // hourly cron (archives the previous hour)
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(archiveToGitHub(env, 1, (ms)=> {
      const d = new Date(ms);
      const pad = (n)=>String(n).padStart(2,"0");
      return `${pad(d.getUTCDate())}/${pad(d.getUTCMonth()+1)}/${d.getUTCFullYear()} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
    }, (raw)=>raw /* toSI injected in function, not used here */).catch(console.error));
  }
};

// ---- GitHub archiver (raw + SI columns) ----
async function archiveToGitHub(env, hours, fmtUTC, toSI) {
  const since = Date.now() - hours * 3600 * 1000;
  const rows = await env.DB.prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC").bind(since).all();

  if (!rows.results.length) return { ok: true, message: "no data in window", path: null };

  const samples = rows.results.map(r => {
    const raw = JSON.parse(r.payload || "{}");
    const si = toSI ? toSI(raw) : {};
    return { ts: r.ts, ts_local: fmtUTC(r.ts), ...raw, ...Object.fromEntries(Object.entries(si).filter(([k]) => !k.startsWith("_"))) };
  });

  const keys = Array.from(samples.reduce((s,o)=>{ Object.keys(o).forEach(k=>s.add(k)); return s; }, new Set(["ts","ts_local"])));
  const esc = (v)=> (v==null ? "" : String(v).replace(/"/g,'""'));
  const csv = [keys.join(","), ...samples.map(s => keys.map(k=>`"${esc(s[k])}"`).join(","))].join("\n");

  const base = env.GH_BASEPATH || "archives/ecowitt";
  const d = new Date(Date.now() - 1 * 3600 * 1000); // prev hour
  const YYYY = d.getUTCFullYear();
  const MM = String(d.getUTCMonth()+1).padStart(2,"0");
  const DD = String(d.getUTCDate()).padStart(2,"0");
  const HH = String(d.getUTCHours()).padStart(2,"0");
  const path = `${base}/${YYYY}/${MM}/${DD}/${YYYY}${MM}${DD}_${HH}00.csv`;

  const repo = env.GH_REPO;
  const branch = env.GH_BRANCH || "main";
  const api = `https://api.github.com/repos/${repo}/contents/${encodeURIComponent(path)}`;
  const headers = {
    "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
    "Accept": "application/vnd.github+json",
    "User-Agent": "andesaware-ecowitt-archiver"
  };

  const head = await fetch(`${api}?ref=${branch}`, { headers });
  if (head.status === 200) return { ok: true, message: "already archived", path };

  const contentB64 = btoa(unescape(encodeURIComponent(csv)));
  const put = await fetch(api, { method: "PUT", headers, body: JSON.stringify({ message: `archive: ${path}`, content: contentB64, branch }) });
  if (!put.ok) {
    const txt = await put.text();
    return { ok: false, message: `github put failed ${put.status}: ${txt}`, path };
  }
  return { ok: true, message: "archived", path };
}
