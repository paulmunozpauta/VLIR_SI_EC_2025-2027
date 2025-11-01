var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/index.js
var index_default = {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const ALLOW = /* @__PURE__ */ new Set(["https://andesaware.com", "https://www.andesaware.com"]);
    const origin = request.headers.get("origin");
    const allowOrigin = ALLOW.has(origin) ? origin : "https://andesaware.com";
    const cors = {
      "Access-Control-Allow-Origin": allowOrigin,
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Vary": "Origin"
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });
    const fmt2 = /* @__PURE__ */ __name((ms) => {
      const d = new Date(ms);
      const pad = /* @__PURE__ */ __name((n) => String(n).padStart(2, "0"), "pad");
      return `${pad(d.getUTCDate())}/${pad(d.getUTCMonth() + 1)}/${d.getUTCFullYear()} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
    }, "fmt");
    if (path === "/") {
      return new Response("andesaware api ready", {
        headers: { "content-type": "text/plain; charset=utf-8", ...cors }
      });
    }
    const save = /* @__PURE__ */ __name(async (data) => {
      const ts = Date.now();
      await env.DB.prepare("INSERT INTO samples (ts, payload) VALUES (?, ?)").bind(ts, JSON.stringify(data)).run();
      return { ts, data };
    }, "save");
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
    const num = /* @__PURE__ */ __name((v) => v == null || v === "" ? null : Number(v), "num");
    const pick = /* @__PURE__ */ __name((o, ...keys) => {
      for (const k of keys) if (o[k] != null) return o[k];
      return null;
    }, "pick");
    const dewpointC = /* @__PURE__ */ __name((tC, rh) => {
      if (tC == null || rh == null) return null;
      const a = 17.62, b = 243.12;
      const gamma = a * tC / (b + tC) + Math.log(rh / 100);
      return b * gamma / (a - gamma);
    }, "dewpointC");
    const toSI2 = /* @__PURE__ */ __name((d) => {
      const tempOutF = num(pick(d, "tempf", "outtempf"));
      const rhOut = num(pick(d, "humidity", "outhumidity"));
      const feelsOutF = num(pick(d, "feelslikef", "heatindexf", "windchillf"));
      const dewOutF = num(pick(d, "dewpointf"));
      const tempOutC = tempOutF != null ? (tempOutF - 32) * 5 / 9 : null;
      const feelsOutC = feelsOutF != null ? (feelsOutF - 32) * 5 / 9 : null;
      const dewOutC = dewOutF != null ? (dewOutF - 32) * 5 / 9 : dewpointC(tempOutC, rhOut);
      const tempInF = num(pick(d, "indoortempf", "tempinf"));
      const rhIn = num(pick(d, "indoorhumidity", "humidityin"));
      const tempInC = tempInF != null ? (tempInF - 32) * 5 / 9 : null;
      return {
        outdoor_temp_c: tempOutC,
        outdoor_feels_like_c: feelsOutC,
        outdoor_dewpoint_c: dewOutC,
        outdoor_humidity_pct: rhOut,
        indoor_temp_c: tempInC,
        indoor_humidity_pct: rhIn,
        solar_wm2: num(pick(d, "solarradiation")),
        uv_index: num(pick(d, "uv")),
        rain_rate_mm_hr: num(pick(d, "rainratein")) * 25.4 || null,
        wind_speed_ms: num(pick(d, "windspeedmph")) * 0.44704 || null,
        pressure_rel_hpa: num(pick(d, "baromrelhpa")),
        pressure_abs_hpa: num(pick(d, "baromabshpa")),
        stationtype: d.stationtype ?? null
      };
    }, "toSI");
    if (path === "/api/latest") {
      const row = await env.DB.prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1").first();
      const out = row ? { ts: row.ts, ts_local: fmt2(row.ts), data: toSI2(JSON.parse(row.payload || "{}")) } : {};
      return new Response(JSON.stringify(out), { headers: { "content-type": "application/json", ...cors } });
    }
    if (path === "/api/latest_raw") {
      const row = await env.DB.prepare("SELECT ts, payload FROM samples ORDER BY id DESC LIMIT 1").first();
      const out = row ? { ts: row.ts, ts_local: fmt2(row.ts), payload: JSON.parse(row.payload || "{}") } : {};
      return new Response(JSON.stringify(out), { headers: { "content-type": "application/json", ...cors } });
    }
    if (path === "/api/export.csv") {
      const rows = await env.DB.prepare("SELECT ts, payload FROM samples ORDER BY ts ASC").all();
      const samples = rows.results.map((r) => {
        const raw = JSON.parse(r.payload || "{}");
        const si = toSI2(raw);
        return {
          ts: r.ts,
          ts_local: fmt2(r.ts),
          ...si,
          ...raw,
          raw_json: JSON.stringify(raw)
        };
      });
      const keys = Array.from(samples.reduce((s, o) => {
        Object.keys(o).forEach((k) => s.add(k));
        return s;
      }, /* @__PURE__ */ new Set(["ts", "ts_local"])));
      const esc = /* @__PURE__ */ __name((v) => v == null ? "" : String(v).replace(/"/g, '""').replace(/,/g, "."), "esc");
      const csv = [keys.join(","), ...samples.map((s) => keys.map((k) => `"${esc(s[k])}"`).join(","))].join("\n");
      return new Response(csv, {
        headers: {
          "content-type": "text/csv; charset=utf-8",
          "content-disposition": `attachment; filename="ecowitt_full.csv"`,
          ...cors
        }
      });
    }
    return new Response("not found", { status: 404, headers: cors });
  },
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(appendToGitHubCSV(env).catch((e) => console.error("archive error", e)));
  }
};
async function appendToGitHubCSV(env) {
  const rows = await env.DB.prepare("SELECT ts, payload FROM samples ORDER BY ts ASC").all();
  if (!rows.results.length) return;
  const samples = rows.results.map((r) => {
    const raw = JSON.parse(r.payload || "{}");
    const si = toSI(raw);
    return {
      ts: r.ts,
      ts_local: fmt(r.ts),
      ...si,
      ...raw,
      raw_json: JSON.stringify(raw)
    };
  });
  const keys = Object.keys(samples[0]);
  const newLines = samples.map((s) => keys.map((k) => `"${(s[k] ?? "").toString().replace(/"/g, '""').replace(/,/g, ".")}"`).join(","));
  const newCSVChunk = newLines.join("\n") + "\n";
  const repo = env.GH_REPO;
  const branch = env.GH_BRANCH || "main";
  const filepath = "archives/ecowitt/ecowitt_history.csv";
  const api = `https://api.github.com/repos/${repo}/contents/${encodeURIComponent(filepath)}`;
  const headers = {
    "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
    "Accept": "application/vnd.github+json",
    "User-Agent": "andesaware-ecowitt-archiver"
  };
  const get = await fetch(`${api}?ref=${branch}`, { headers });
  let sha, oldContent = "";
  if (get.status === 200) {
    const json = await get.json();
    sha = json.sha;
    oldContent = atob(json.content.replace(/\n/g, ""));
  }
  const contentB64 = btoa(unescape(encodeURIComponent(oldContent + newCSVChunk)));
  const body = {
    message: `update: ${filepath}`,
    content: contentB64,
    branch,
    ...sha ? { sha } : {}
  };
  const put = await fetch(api, { method: "PUT", headers, body: JSON.stringify(body) });
  if (!put.ok) {
    const txt = await put.text();
    return { ok: false, message: `github put failed ${put.status}: ${txt}`, path: filepath };
  }
  return { ok: true, message: "appended", path: filepath };
}
__name(appendToGitHubCSV, "appendToGitHubCSV");
export {
  index_default as default
};
//# sourceMappingURL=index.js.map
