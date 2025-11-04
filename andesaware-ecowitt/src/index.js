export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    // preflight
    if (req.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders(req) });
    }

    // --- READ: latest ---
    if (url.pathname === "/weatherstation/latest") {
      const j = await env.WEATHER_KV.get("latest.json");
      if (!j) {
        return new Response("not found", { status: 404, headers: { ...corsHeaders(req), "Cache-Control": "no-store" } });
      }
      return new Response(j, {
        status: 200,
        headers: { ...corsHeaders(req), "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" },
      });
    }

    // --- WRITE: updateweatherstation (GET or POST form) ---
    let params = Object.fromEntries(url.searchParams.entries());
    if (req.method === "POST") {
      const ct = req.headers.get("content-type") || "";
      if (ct.includes("application/x-www-form-urlencoded")) {
        params = Object.fromEntries(new URLSearchParams(await req.text()).entries());
      } else if (ct.includes("application/json")) {
        try { params = { ...params, ...(await req.json()) }; } catch {}
      }
    } else if (req.method !== "GET") {
      return plain("method not allowed", 405, req);
    }

    const id  = params.ID || "";
    const key = params.PASSWORD || "";
    if (id !== env.STATION_ID || key !== env.STATION_KEY) {
      console.log("auth_fail", { id });
      return plain("unauthorized", 403, req);
    }

    params.received_at = new Date().toISOString();
    const payload = JSON.stringify({ wu_raw: params }, null, 2);

    try {
      await env.WEATHER_KV.put("latest.json", payload);
      console.log("kv_written", { at: params.received_at, keys: Object.keys(params).length });
    } catch (e) {
      console.log("kv_error", e.toString());
      return plain("kv write failed", 500, req);
    }

    // exact reply for ecowitt
    return plain("success", 200, req);
  }
};

function plain(body, status, req) {
  return new Response(body, {
    status,
    headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store", ...corsHeaders(req) },
  });
}
function corsHeaders(req) {
  const origin = req.headers.get("origin") || "";
  const allow = ["https://andesaware.com","https://www.andesaware.com","https://paulmunozpauta.github.io"].includes(origin) ? origin : "*";
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
  };
}
