export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const path = url.pathname;

    console.log("start", { path, method: req.method });

    // CORS
    if (req.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders(req) });
    }

    // human status
    if (path === "/weatherstation/status") {
      const j = await env.WEATHER_KV.get("latest.json");
      const last = j ? (() => { try { return JSON.parse(j)?.received_at; } catch { return null; } })() : null;
      const age = last ? Math.round((Date.now() - Date.parse(last)) / 1000) : null;
      const ok = age != null && age < 180;
      return new Response(ok ? `OK: last=${last} age=${age}s` : "STALE", { status: ok ? 200 : 503, headers: noStore(req) });
    }

    // raw latest
    if (path === "/weatherstation/latest") {
      const j = await env.WEATHER_KV.get("latest.json");
      console.log("read_latest", { found: !!j, len: j?.length || 0 });
      if (!j) return new Response("not found", { status: 404, headers: noStoreJson(req) });
      return new Response(j, { status: 200, headers: noStoreJson(req) });
    }

    // accept Ecowitt data
    const isEcowitt =
      path === "/weatherstation/updateweatherstation" ||
      path === "/weatherstation/updateweatherstation/" ||
      path === "/weatherstation/updateweatherstation.php" ||
      path === "/weatherstation/updateweatherstation.php/";

    if (!isEcowitt) {
      console.log("unknown_path", { path, method: req.method });
      return new Response("not found", { status: 404, headers: noStore(req) });
    }

    // Ecowitt protocol - usually POST with JSON body
    let params = {};
    
    if (req.method === "POST") {
      const ct = (req.headers.get("content-type") || "").toLowerCase();
      if (ct.includes("application/json")) {
        try { 
          params = await req.json();
        } catch (e) {
          console.log("json_parse_error", { error: e.message });
        }
      } else if (ct.includes("application/x-www-form-urlencoded")) {
        params = Object.fromEntries(new URLSearchParams(await req.text()).entries());
      }
    } else if (req.method === "GET") {
      params = Object.fromEntries(url.searchParams.entries());
    } else {
      return new Response("method not allowed", { status: 405, headers: noStore(req) });
    }

    console.log("received_data", { method: req.method, keys: Object.keys(params).length });

    // For Ecowitt protocol, you might want different authentication
    // For now, let's accept all data and log it
    params.received_at = new Date().toISOString();
    const payload = JSON.stringify(params, null, 2);
    
    await env.WEATHER_KV.put("latest.json", payload);
    console.log("kv_written", { at: params.received_at, keys: Object.keys(params).length });

    // Ecowitt expects simple response
    return new Response("success", { status: 200, headers: noStore(req) });
  }
};

function corsHeaders(req) {
  const origin = req.headers.get("origin") || "";
  const allow = ["https://andesaware.com","https://www.andesaware.com","https://paulmunozpauta.github.io"].includes(origin) ? origin : "*";
  return { "Access-Control-Allow-Origin": allow, "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Max-Age": "86400" };
}
function noStore(req){ return { ...corsHeaders(req), "Cache-Control":"no-store", "Content-Type": "text/plain; charset=utf-8" }; }
function noStoreJson(req){ return { ...corsHeaders(req), "Cache-Control":"no-store", "Content-Type": "application/json; charset=utf-8" }; }