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
      const ok = age != null && age < 600; // 10 minutes tolerance
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

  async scheduled(event, env, ctx) {
    console.log("Fetching data from Ecowitt API (5-minute interval)...");
    
    try {
      const mac = "EC:64:C9:F2:AC:79";
      const apiUrl = `https://api.ecowitt.net/api/v3/device/real_time?application_key=31B06CAD6518B81F808312D91B55973A&api_key=be6a3fde-4a04-40e0-8452-49ef32af65a0&mac=${mac}&call_back=all&temp_unitid=2&pressure_unitid=4&wind_speed_unitid=9&rainfall_unitid=13`;
      
      const response = await fetch(apiUrl);
      const data = await response.json();
      
      console.log("Ecowitt API response:", { code: data.code, msg: data.msg });
      
      if (data.code === 0) {
        const timestamp = new Date().toISOString();
        
        // Store in KV
        const payload = JSON.stringify({
          ecowitt_api: data.data,
          received_at: timestamp,
          api_timestamp: data.time
        }, null, 2);
        
        await env.WEATHER_KV.put("latest.json", payload);
        
        // Append to CSV in GitHub in datasets/AW001.csv every 5 minutes
        await appendToGitHubCSV(data.data, timestamp, env);
        
        console.log("5-minute data stored successfully", { 
          outdoor_temp: data.data.outdoor?.temperature?.value,
          humidity: data.data.outdoor?.humidity?.value,
          timestamp: timestamp
        });
      } else {
        console.log("Ecowitt API error:", data.msg, data.code);
      }
    } catch (error) {
      console.log("Ecowitt API fetch failed:", error.message);
    }
  }
};

// Function to append data to CSV in GitHub in datasets/AW001.csv
async function appendToGitHubCSV(weatherData, timestamp, env) {
  try {
    // Extract main weather parameters
    const outdoor = weatherData.outdoor || {};
    const wind = weatherData.wind || {};
    const pressure = weatherData.pressure || {};
    const rainfall = weatherData.rainfall || {};
    const solar_uvi = weatherData.solar_and_uvi || {};
    const indoor = weatherData.indoor || {};
    
    // Create CSV row
    const csvRow = [
      timestamp,
      outdoor.temperature?.value || '',
      outdoor.humidity?.value || '',
      outdoor.dew_point?.value || '',
      outdoor.feels_like?.value || '',
      wind.wind_speed?.value || '',
      wind.wind_gust?.value || '',
      wind.wind_direction?.value || '',
      pressure.relative?.value || '',
      rainfall.rain_rate?.value || '',
      rainfall.daily?.value || '',
      solar_uvi.solar?.value || '',
      solar_uvi.uvi?.value || '',
      indoor.temperature?.value || '',
      indoor.humidity?.value || ''
    ].join(',');
    
    // FIXED: Correct GitHub API URL - removed duplicate username
    const csvUrl = `https://api.github.com/repos/${env.GITHUB_REPO}/contents/datasets/AW001.csv`;
    const headers = {
      'Authorization': `token ${env.GITHUB_TOKEN}`,
      'User-Agent': 'Cloudflare-Worker',
      'Accept': 'application/vnd.github.v3+json'
    };
    
    let existingContent = '';
    let sha = null;
    
    try {
      const existingFile = await fetch(csvUrl, { headers });
      if (existingFile.ok) {
        const fileData = await existingFile.json();
        existingContent = atob(fileData.content); // Decode base64
        sha = fileData.sha;
      }
    } catch (e) {
      console.log("No existing CSV file or error reading:", e.message);
    }
    
    // Create CSV header if file doesn't exist
    if (!existingContent) {
      existingContent = 'timestamp,temperature_f,humidity_pct,dew_point_f,feels_like_f,wind_speed_mph,wind_gust_mph,wind_direction_deg,pressure_inhg,rain_rate_inhr,daily_rain_in,solar_wm2,uv_index,indoor_temp_f,indoor_humidity_pct\n';
    }
    
    // Append new row
    const newContent = existingContent + csvRow + '\n';
    
    // Update file in GitHub
    const updateResponse = await fetch(csvUrl, {
      method: 'PUT',
      headers,
      body: JSON.stringify({
        message: `Add 5-minute weather data: ${timestamp}`,
        content: btoa(newContent), // Encode to base64
        sha: sha
      })
    });
    
    if (updateResponse.ok) {
      console.log("5-minute CSV data appended to GitHub successfully");
    } else {
      const error = await updateResponse.text();
      console.log("Failed to update GitHub CSV:", error);
    }
    
  } catch (error) {
    console.log("Error appending to GitHub CSV:", error.message);
  }
}

function corsHeaders(req) {
  const origin = req.headers.get("origin") || "";
  const allow = ["https://andesaware.com","https://www.andesaware.com","https://paulmunozpauta.github.io"].includes(origin) ? origin : "*";
  return { "Access-Control-Allow-Origin": allow, "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Max-Age": "86400" };
}
function noStore(req){ return { ...corsHeaders(req), "Cache-Control":"no-store", "Content-Type": "text/plain; charset=utf-8" }; }
function noStoreJson(req){ return { ...corsHeaders(req), "Cache-Control":"no-store", "Content-Type": "application/json; charset=utf-8" }; }