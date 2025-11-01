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

    // fallback
    return new Response("not found", { status: 404, headers: cors });
  }
}
