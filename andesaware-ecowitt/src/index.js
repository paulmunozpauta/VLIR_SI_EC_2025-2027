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

    // landing page
    if (path === "/") {
      return new Response("andesaware api ready", {
        headers: { "content-type": "text/plain; charset=utf-8", ...cors }
      });
    }

    // helper to save raw payload
    const save = async (data) => {
      const ts = Date.now();
      await env.DB.prepare("INSERT INTO samples (ts, payload) VALUES (?, ?)")
        .bind(ts, JSON.stringify(data)).run();
      return { ts, data };
    };

    // -------------- /api/ecowitt --------------
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
      } else {
        return new Response("method not allowed", { status: 405, headers: cors });
      }

      if (env.ECOWITT_PASSKEY && params.passkey !== env.ECOWITT_PASSKEY) {
        return new Response("bad passkey", { status: 401, headers: cors });
      }

      await save(params);
      console.log("rx", new Date().toISOString(), params.stationtype, params.tempf, params.humidity);
      return new Response("OK", { headers: cors });
    }

    // -------------- helpers for SI transforms --------------
    const toSI = (d) => ({
      stationtype: d.stationtype ?? null,
      temp_c: d.tempf != null ? (Number(d.tempf) - 32) * 5 / 9 : null,
      rh: d.humidity != null ? Number(d.humidity) : null,
      wind_ms: d.windspeedmph != null ? Number(d.windspeedmph) * 0.44704 : null,
      wind_dir_deg: d.winddir != null ? Number(d.winddir) : null,
      rain_mm_hr: d.rainratein != null ? Number(d.rainratein) * 25.4 : null,
      solar_wm2: d.solarradiation != null ? Number(d.solarradiation) : null,
      uv: d.uv != null ? Number(d.uv) : null,
      pressure_hpa: d.baromabsin != null ? Number(d.baromabsin) * 33.8639 : null
    });

    // -------------- /api/latest (SI) --------------
    if (path === "/api/latest") {
      const row = await env.DB
        .prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1")
        .first();

      const out = row
        ? { ts: row.ts, data: toSI(JSON.parse(row.payload || "{}")) }
        : {};

      return new Response(JSON.stringify(out), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // -------------- /api/history (SI) --------------
    if (path === "/api/history") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB
        .prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC")
        .bind(since).all();

      const series = rows.results.map(r => {
        const d = JSON.parse(r.payload || "{}");
        return { t: r.ts, ...toSI(d) };
      });

      return new Response(JSON.stringify(series), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // -------------- /api/latest_raw --------------
    if (path === "/api/latest_raw") {
      const row = await env.DB
        .prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1")
        .first();
      const out = row ? { ts: row.ts, payload: JSON.parse(row.payload || "{}") } : {};
      return new Response(JSON.stringify(out), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // -------------- /api/history_raw --------------
    if (path === "/api/history_raw") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB
        .prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC")
        .bind(since).all();

      const out = rows.results.map(r => ({ ts: r.ts, payload: JSON.parse(r.payload || "{}") }));
      return new Response(JSON.stringify(out), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // -------------- /api/export.csv --------------
    if (path === "/api/export.csv") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB
        .prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC")
        .bind(since).all();

      const samples = rows.results.map(r => ({ ts: r.ts, ...JSON.parse(r.payload || "{}") }));
      const keys = Array.from(samples.reduce((s, o) => { Object.keys(o).forEach(k => s.add(k)); return s; }, new Set(["ts"])));

      const esc = v => (v == null ? "" : String(v).replace(/"/g, '""'));
      const header = keys.join(",");
      const lines = samples.map(s => keys.map(k => `"${esc(s[k])}"`).join(","));
      const csv = [header, ...lines].join("\n");

      return new Response(csv, {
        headers: {
          "content-type": "text/csv; charset=utf-8",
          "content-disposition": `attachment; filename="ecowitt_${Date.now()}.csv"`,
          ...cors
        }
      });
    }

    // -------------- /api/health --------------
    if (path === "/api/health") {
      const row = await env.DB
        .prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1")
        .first();
      const now = Date.now();
      let status = "no-data", lag_s = null, last = null;
      if (row) {
        lag_s = Math.round((now - row.ts) / 1000);
        status = lag_s <= 120 ? "ok" : "stale";
        last = JSON.parse(row.payload || "{}");
      }
      return new Response(JSON.stringify({
        status, now, last_ts: row?.ts ?? null, lag_s, last
      }), { headers: { "content-type": "application/json", ...cors } });
    }

    // -------------- /api/stats --------------
    if (path === "/api/stats") {
      const hours = Number(url.searchParams.get("hours") || "24");
      const since = Date.now() - hours * 3600 * 1000;
      const rows = await env.DB
        .prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC")
        .bind(since).all();

      const series = rows.results.map(r => toSI(JSON.parse(r.payload || "{}")));

      const fields = ["temp_c", "wind_ms", "rain_mm_hr", "pressure_hpa", "rh", "solar_wm2", "uv"];
      const stats = {};
      for (const k of fields) {
        const vals = series.map(s => s[k]).filter(v => v != null && !Number.isNaN(v));
        if (vals.length) {
          const sum = vals.reduce((a, b) => a + b, 0);
          stats[k] = {
            count: vals.length,
            min: Math.min(...vals),
            max: Math.max(...vals),
            avg: sum / vals.length
          };
        } else {
          stats[k] = { count: 0, min: null, max: null, avg: null };
        }
      }

      return new Response(JSON.stringify({ hours, n: rows.results.length, stats }), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // -------------- manual archive trigger: /api/archive.now?hours=1 --------------
    if (path === "/api/archive.now") {
      const hours = Number(url.searchParams.get("hours") || "1");
      const res = await archiveToGitHub(env, hours);
      return new Response(JSON.stringify(res), {
        headers: { "content-type": "application/json", ...cors }
      });
    }

    // fallback
    return new Response("not found", { status: 404, headers: cors });
  },

  // -------------- hourly cron (archives previous hour) --------------
  async scheduled(controller, env, ctx) {
    // archive last hour each run
    ctx.waitUntil(archiveToGitHub(env, 1).then(r => {
      console.log("archive result", r.ok, r.path, r.message);
    }).catch(e => console.error("archive error", e)));
  }
}

/** Build CSV and commit to GitHub */
async function archiveToGitHub(env, hours) {
  const since = Date.now() - hours * 3600 * 1000;
  const rows = await env.DB
    .prepare("SELECT ts, payload FROM samples WHERE ts>=? ORDER BY ts ASC")
    .bind(since).all();

  // flatten rows
  const samples = rows.results.map(r => ({ ts: r.ts, ...JSON.parse(r.payload || "{}") }));
  if (!samples.length) return { ok: true, message: "no data in window", path: null };

  // collect headers dynamically
  const keys = Array.from(samples.reduce((s, o) => { Object.keys(o).forEach(k => s.add(k)); return s; }, new Set(["ts"])));
  const esc = v => (v == null ? "" : String(v).replace(/"/g, '""'));
  const header = keys.join(",");
  const lines = samples.map(s => keys.map(k => `"${esc(s[k])}"`).join(","));
  const csv = [header, ...lines].join("\n");

  // path: archives/ecowitt/YYYY/MM/DD/YYYYMMDD_HH00.csv (UTC)
  const base = env.GH_BASEPATH || "archives/ecowitt";
  const d = new Date(Date.now() - 1 * 3600 * 1000); // previous hour
  const YYYY = d.getUTCFullYear();
  const MM = String(d.getUTCMonth() + 1).padStart(2, "0");
  const DD = String(d.getUTCDate()).padStart(2, "0");
  const HH = String(d.getUTCHours()).padStart(2, "0");
  const path = `${base}/${YYYY}/${MM}/${DD}/${YYYY}${MM}${DD}_${HH}00.csv`;

  // skip if file already exists
  const repo = env.GH_REPO;            // "owner/repo"
  const branch = env.GH_BRANCH || "main";
  const api = `https://api.github.com/repos/${repo}/contents/${encodeURIComponent(path)}`;
  const headers = {
    "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
     "Accept": "application/vnd.github+json",
     "User-Agent": "andesaware-ecowitt-archiver"
   };
  const head = await fetch(`${api}?ref=${branch}`, { headers });
  if (head.status === 200) {
    return { ok: true, message: "already archived", path };
  }

  // commit file
  const contentB64 = btoa(unescape(encodeURIComponent(csv)));
  const body = {
    message: `archive: ${path}`,
    content: contentB64,
    branch
  };
  const put = await fetch(api, { method: "PUT", headers, body: JSON.stringify(body) });
  if (!put.ok) {
    const txt = await put.text();
    return { ok: false, message: `github put failed ${put.status}: ${txt}`, path };
  }
  return { ok: true, message: "archived", path };
}
