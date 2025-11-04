// src/index.js
export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    // --- CORS preflight ---
    if (req.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders(req) });
    }

    // --- OPTIONAL read endpoint to verify KV from your browser ---
    if (url.pathname === "/weatherstation/latest") {
      const j = await env.WEATHER_KV.get("latest.json");
      if (!j) return new Response("not found", { status: 404, headers: corsHeaders(req) });
      return new Response(j, {
        status: 200,
        headers: { ...corsHeaders(req), "Content-Type": "application/json; charset=utf-8" },
      });
    }

    // wunderground usually sends GET, but accept POST form too
    let params = Object.fromEntries(url.searchParams.entries());
    if (req.method === "POST") {
      const ct = req.headers.get("content-type") || "";
      if (ct.includes("application/x-www-form-urlencoded")) {
        const body = await req.text();
        params = Object.fromEntries(new URLSearchParams(body).entries());
      } else if (ct.includes("application/json")) {
        try { params = { ...params, ...(await req.json()) }; } catch (_) {}
      }
    } else if (req.method !== "GET") {
      return text("method not allowed", 405, req);
    }

    const id  = params.ID || "";
    const key = params.PASSWORD || "";
    if (id !== env.STATION_ID || key !== env.STATION_KEY) {
      return text("unauthorized", 403, req);
    }

    params.received_at = new Date().toISOString();
    const payload = JSON.stringify({ wu_raw: params }, null, 2);
    await env.WEATHER_KV.put("latest.json", payload);

    // exact response required by console
    return text("success", 200, req);
  }
};

function text(body, status, req) {
  return new Response(body, {
    status,
    headers: { "Content-Type": "text/plain; charset=utf-8", ...corsHeaders(req) },
  });
}
function corsHeaders(req) {
  const origin = req.headers.get("origin") || "";
  const allow = whitelist.includes(origin) ? origin : "*";
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
  };
}
const whitelist = [
  "https://andesaware.com",
  "https://www.andesaware.com",
  "https://paulmunozpauta.github.io",
];
