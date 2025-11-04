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
      const ok = age != null && age < 300; // 5 minutes
      return new Response(ok ? `OK: last=${last} age=${age}s` : "STALE", { status: ok ? 200 : 503, headers: noStore(req) });
    }

    // raw latest
    if (path === "/weatherstation/latest") {
      const j = await env.WEATHER_KV.get("latest.json");
      console.log("read_latest", { found: !!j, len: j?.length || 0 });
      if (!j) return new Response("not found", { status: 404, headers: noStoreJson(req) });
      return new Response(j, { status: 200, headers: noStoreJson(req) });
    }

    return new Response("not found", { status: 404, headers: noStore(req) });
  },

  // Scheduled function to fetch from Ecowitt API every 2 minutes
  async scheduled(event, env, ctx) {
    console.log("Fetching data from Ecowitt API...");
    
    try {
      // Replace YOUR_DEVICE_MAC with your actual MAC address
      const mac = "EC:64:C9:F2:AC:79"; // ← CHANGE THIS to your station's MAC
      
      const apiUrl = `https://api.ecowitt.net/api/v3/device/real_time?application_key=31B06CAD6518B81F808312D91B55973A&api_key=be6a3fde-4a04-40e0-8452-49ef32af65a0&mac=${mac}&call_back=all&temp_unitid=2&pressure_unitid=4&wind_speed_unitid=9&rainfall_unitid=13`;
      
      console.log("Calling Ecowitt API:", apiUrl);
      
      const response = await fetch(apiUrl);
      const data = await response.json();
      
      console.log("Ecowitt API response:", { code: data.code, msg: data.msg });
      
      if (data.code === 0) {
        // Success - store the data
        const payload = JSON.stringify({
          ecowitt_api: data.data,
          received_at: new Date().toISOString(),
          api_timestamp: data.time
        }, null, 2);
        
        await env.WEATHER_KV.put("latest.json", payload);
        console.log("Ecowitt API data stored successfully", { 
          outdoor_temp: data.data.outdoor?.temperature?.value,
          humidity: data.data.outdoor?.humidity?.value 
        });
      } else {
        console.log("Ecowitt API error:", data.msg, data.code);
      }
    } catch (error) {
      console.log("Ecowitt API fetch failed:", error.message);
    }
  }
};

function corsHeaders(req) {
  const origin = req.headers.get("origin") || "";
  const allow = ["https://andesaware.com","https://www.andesaware.com","https://paulmunozpauta.github.io"].includes(origin) ? origin : "*";
  return { "Access-Control-Allow-Origin": allow, "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Max-Age": "86400" };
}
function noStore(req){ return { ...corsHeaders(req), "Cache-Control":"no-store", "Content-Type": "text/plain; charset=utf-8" }; }
function noStoreJson(req){ return { ...corsHeaders(req), "Cache-Control":"no-store", "Content-Type": "application/json; charset=utf-8" }; }