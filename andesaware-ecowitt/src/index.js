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
      const ok = age != null && age < 600;
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
        
        // Append to CSV in GitHub
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

// Function to append data to CSV in GitHub
async function appendToGitHubCSV(weatherData, timestamp, env) {
  try {
    console.log("=== GITHUB CSV DEBUG START ===");
    
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
    
    // GitHub configuration
    const repo = env.GITHUB_REPO; // Should be "paulmunozpauta/VLIR_SI_EC_2025-2027"
    const csvUrl = `https://api.github.com/repos/${repo}/contents/datasets/AW001.csv`;
    
    console.log("GitHub Config:", {
      repo: repo,
      fullUrl: csvUrl,
      tokenExists: !!env.GITHUB_TOKEN,
      tokenLength: env.GITHUB_TOKEN ? env.GITHUB_TOKEN.length : 0
    });
    
    const headers = {
      'Authorization': `token ${env.GITHUB_TOKEN}`,
      'User-Agent': 'Cloudflare-Worker',
      'Accept': 'application/vnd.github.v3+json'
    };
    
    // Test 1: Check if repository exists and is accessible
    console.log("Testing repository access...");
    const repoTestUrl = `https://api.github.com/repos/${repo}`;
    const repoResponse = await fetch(repoTestUrl, { headers });
    console.log("Repository test - Status:", repoResponse.status);
    
    if (!repoResponse.ok) {
      const repoError = await repoResponse.text();
      console.log("❌ Repository access failed:", repoError);
      return;
    }
    console.log("✅ Repository access successful");
    
    // Test 2: Check if datasets folder exists or create it
    console.log("Checking datasets folder...");
    const datasetsUrl = `https://api.github.com/repos/${repo}/contents/datasets`;
    const datasetsResponse = await fetch(datasetsUrl, { headers });
    console.log("Datasets folder check - Status:", datasetsResponse.status);
    
    let existingContent = '';
    let sha = null;
    
    // Test 3: Check if CSV file exists
    console.log("Checking if AW001.csv exists...");
    const fileResponse = await fetch(csvUrl, { headers });
    console.log("File check - Status:", fileResponse.status);
    
    if (fileResponse.ok) {
      const fileData = await fileResponse.json();
      existingContent = atob(fileData.content);
      sha = fileData.sha;
      console.log("✅ Found existing CSV file, rows:", existingContent.split('\n').length - 1);
    } else if (fileResponse.status === 404) {
      console.log("📝 CSV file doesn't exist, will create new file");
    } else {
      const errorText = await fileResponse.text();
      console.log("❌ File check error:", fileResponse.status, errorText);
      return;
    }
    
    // Create CSV header if file doesn't exist
    if (!existingContent) {
      existingContent = 'timestamp,temperature_f,humidity_pct,dew_point_f,feels_like_f,wind_speed_mph,wind_gust_mph,wind_direction_deg,pressure_inhg,rain_rate_inhr,daily_rain_in,solar_wm2,uv_index,indoor_temp_f,indoor_humidity_pct\n';
      console.log("📄 Created CSV headers");
    }
    
    // Append new row
    const newContent = existingContent + csvRow + '\n';
    
    console.log("Updating file on GitHub...");
    const updateResponse = await fetch(csvUrl, {
      method: 'PUT',
      headers,
      body: JSON.stringify({
        message: `Add 5-minute weather data: ${timestamp}`,
        content: btoa(newContent),
        sha: sha
      })
    });
    
    console.log("GitHub update response - Status:", updateResponse.status);
    
    if (updateResponse.ok) {
      console.log("✅ CSV data appended to GitHub successfully");
    } else {
      const error = await updateResponse.text();
      console.log("❌ Failed to update GitHub CSV. Status:", updateResponse.status, "Error:", error);
    }
    
    console.log("=== GITHUB CSV DEBUG END ===");
    
  } catch (error) {
    console.log("💥 Error in appendToGitHubCSV:", error.message, error.stack);
  }
}

function corsHeaders(req) {
  const origin = req.headers.get("origin") || "";
  const allow = ["https://andesaware.com","https://www.andesaware.com","https://paulmunozpauta.github.io"].includes(origin) ? origin : "*";
  return { "Access-Control-Allow-Origin": allow, "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Max-Age": "86400" };
}
function noStore(req){ return { ...corsHeaders(req), "Cache-Control":"no-store", "Content-Type": "text/plain; charset=utf-8" }; }
function noStoreJson(req){ return { ...corsHeaders(req), "Cache-Control":"no-store", "Content-Type": "application/json; charset=utf-8" }; }