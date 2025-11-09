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
      const ok = age != null && age < 3600; // Changed from 600 to 3600 (1 hour)
      return new Response(ok ? `OK: last=${last} age=${age}s` : "STALE", { status: ok ? 200 : 503, headers: noStore(req) });
    }

    // raw latest - BACKWARD COMPATIBLE: returns AW001 data
    if (path === "/weatherstation/latest") {
      const j = await env.WEATHER_KV.get("sensor_AW001.json");
      console.log("read_latest (AW001)", { found: !!j, len: j?.length || 0 });
      if (!j) return new Response("not found", { status: 404, headers: noStoreJson(req) });
      return new Response(j, { status: 200, headers: noStoreJson(req) });
    }

    // individual sensor data - NEW ENDPOINTS
    if (path.startsWith("/weatherstation/sensor/")) {
      const sensorId = path.split('/').pop();
      const j = await env.WEATHER_KV.get(`sensor_${sensorId}.json`);
      console.log("read_sensor", { sensorId, found: !!j });
      if (!j) return new Response("sensor not found", { status: 404, headers: noStoreJson(req) });
      return new Response(j, { status: 200, headers: noStoreJson(req) });
    }

    return new Response("not found", { status: 404, headers: noStore(req) });
  },

  async scheduled(event, env, ctx) {
    console.log("Fetching data from Ecowitt API for all sensors (HOURLY interval)...");
    
    // Check if we should run this hour (reduce GitHub API calls)
    const now = new Date();
    const currentMinute = now.getMinutes();
    
    // Only run at the top of the hour (minute 0-5) to reduce API calls
    if (currentMinute > 5) {
      console.log(`Skipping - not at top of hour (current minute: ${currentMinute})`);
      return;
    }
    
    // ⚠️ IMPORTANT: REPLACE THESE MAC ADDRESSES WITH YOUR ACTUAL SENSOR MACs ⚠️
    const sensors = [
      { 
        id: "AW001", 
        mac: "EC:64:C9:F2:AC:79", 
        name: "Kessel-Lo Main Station",
        csvFile: "AW001.csv",
        location: "Kessel-Lo, Belgium",
        coordinates: [50.90241987188318, 4.720330533578788]
      },
      { 
        id: "AW002", 
        mac: "CC:7B:5C:51:58:E1",
        name: "AW002 Location Name",
        csvFile: "AW002.csv",
        location: "AW002 Location",
        coordinates: [51.000000, 4.000000]
      },
      { 
        id: "AW003", 
        mac: "24:D7:EB:EA:E5:EC",
        name: "AW003 Location Name", 
        csvFile: "AW003.csv",
        location: "AW003 Location",
        coordinates: [51.100000, 4.100000]
      },
      { 
        id: "AW004", 
        mac: "EC:64:C9:F1:CC:74",
        name: "AW004 Location Name",
        csvFile: "AW004.csv",
        location: "AW004 Location",
        coordinates: [51.200000, 4.200000]
      },
      { 
        id: "AW005", 
        mac: "CC:7B:5C:51:55:E9",
        name: "AW005 Location Name",
        csvFile: "AW005.csv",
        location: "AW005 Location",
        coordinates: [51.300000, 4.300000]
      }
    ];

    try {
      // Process sensors sequentially instead of parallel to reduce burst load
      const results = [];
      for (const sensor of sensors) {
        try {
          // Add delay between sensor requests to avoid rate limits
          if (sensor.id !== "AW001") {
            await new Promise(resolve => setTimeout(resolve, 2000)); // 2 second delay
          }
          
          const apiUrl = `https://api.ecowitt.net/api/v3/device/real_time?application_key=31B06CAD6518B81F808312D91B55973A&api_key=be6a3fde-4a04-40e0-8452-49ef32af65a0&mac=${sensor.mac}&call_back=all&temp_unitid=2&pressure_unitid=4&wind_speed_unitid=9&rainfall_unitid=13`;
          
          console.log(`Fetching HOURLY data for sensor ${sensor.id}...`);
          const response = await fetch(apiUrl);
          
          if (!response.ok) {
            console.log(`Sensor ${sensor.id} HTTP error: ${response.status}`);
            results.push({ sensorId: sensor.id, success: false, error: `HTTP ${response.status}` });
            continue;
          }
          
          const data = await response.json();
          
          if (data.code === 0) {
            const timestamp = new Date().toISOString();
            
            const payload = JSON.stringify({
              sensor_id: sensor.id,
              sensor_name: sensor.name,
              sensor_location: sensor.location,
              sensor_coordinates: sensor.coordinates,
              ecowitt_api: data.data,
              received_at: timestamp,
              api_timestamp: data.time,
              collection_type: "HOURLY" // Mark as hourly collection
            }, null, 2);
            
            // Store individual sensor data
            await env.WEATHER_KV.put(`sensor_${sensor.id}.json`, payload);
            
            // Only append to GitHub CSV once per hour to reduce API calls
            if (currentMinute <= 2) { // Only in first 2 minutes of the hour
              await appendToGitHubCSV(data.data, timestamp, env, sensor.csvFile, sensor.id);
            } else {
              console.log(`Skipping GitHub update for ${sensor.id} - not in CSV window`);
            }
            
            console.log(`Sensor ${sensor.id} HOURLY data stored successfully`);
            results.push({ sensorId: sensor.id, success: true, data: JSON.parse(payload) });
          } else {
            console.log(`Sensor ${sensor.id} API error:`, data.msg);
            results.push({ sensorId: sensor.id, success: false, error: data.msg });
          }
        } catch (error) {
          console.log(`Sensor ${sensor.id} fetch failed:`, error.message);
          results.push({ sensorId: sensor.id, success: false, error: error.message });
        }
      }

      // Create combined latest data
      const combinedData = {
        timestamp: new Date().toISOString(),
        collection_type: "HOURLY",
        sensors: results.filter(r => r.success).map(r => ({
          sensor_id: r.sensorId,
          sensor_name: r.data.sensor_name,
          sensor_location: r.data.sensor_location,
          sensor_coordinates: r.data.sensor_coordinates,
          data: r.data.ecowitt_api,
          received_at: r.data.received_at
        }))
      };
      
      await env.WEATHER_KV.put("latest.json", JSON.stringify(combinedData, null, 2));
      
      const successful = results.filter(r => r.success).length;
      console.log(`HOURLY job completed: ${successful}/5 sensors successful`);
      
    } catch (error) {
      console.log("HOURLY job failed:", error.message);
    }
  }
};

// Function to append data to CSV in GitHub
async function appendToGitHubCSV(weatherData, timestamp, env, csvFileName, sensorId) {
  try {
    console.log(`=== GITHUB CSV HOURLY UPDATE for ${sensorId} ===`);
    
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
    const repo = env.GITHUB_REPO;
    const csvUrl = `https://api.github.com/repos/${repo}/contents/datasets/${csvFileName}`;
    
    const headers = {
      'Authorization': `token ${env.GITHUB_TOKEN}`,
      'User-Agent': 'Cloudflare-Worker',
      'Accept': 'application/vnd.github.v3+json'
    };
    
    let existingContent = '';
    let sha = null;
    
    // Check if CSV file exists
    const fileResponse = await fetch(csvUrl, { headers });
    
    if (fileResponse.ok) {
      const fileData = await fileResponse.json();
      existingContent = atob(fileData.content);
      sha = fileData.sha;
      console.log(`✅ Found existing CSV for ${sensorId}, rows:`, existingContent.split('\n').length - 1);
    } else if (fileResponse.status === 404) {
      console.log(`📝 Creating new CSV file for ${sensorId}`);
    } else {
      console.log(`❌ File check error for ${sensorId}:`, fileResponse.status);
      return;
    }
    
    // Create CSV header if file doesn't exist
    if (!existingContent) {
      existingContent = 'timestamp,temperature_f,humidity_pct,dew_point_f,feels_like_f,wind_speed_mph,wind_gust_mph,wind_direction_deg,pressure_inhg,rain_rate_inhr,daily_rain_in,solar_wm2,uv_index,indoor_temp_f,indoor_humidity_pct\n';
    }
    
    // Append new row
    const newContent = existingContent + csvRow + '\n';
    
    const updateResponse = await fetch(csvUrl, {
      method: 'PUT',
      headers,
      body: JSON.stringify({
        message: `Add HOURLY weather data for ${sensorId}: ${timestamp}`,
        content: btoa(newContent),
        sha: sha
      })
    });
    
    if (updateResponse.ok) {
      console.log(`✅ HOURLY CSV data for ${sensorId} appended to GitHub`);
    } else {
      console.log(`❌ Failed to update GitHub CSV for ${sensorId}:`, updateResponse.status);
    }
    
  } catch (error) {
    console.log(`💥 Error in appendToGitHubCSV for ${sensorId}:`, error.message);
  }
}

function corsHeaders(req) {
  const origin = req.headers.get("origin") || "";
  const allow = ["https://andesaware.com","https://www.andesaware.com","https://paulmunozpauta.github.io"].includes(origin) ? origin : "*";
  return { "Access-Control-Allow-Origin": allow, "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Max-Age": "86400" };
}

function noStore(req){ 
  return { ...corsHeaders(req), "Cache-Control":"no-store", "Content-Type": "text/plain; charset=utf-8" }; 
}

function noStoreJson(req){ 
  return { ...corsHeaders(req), "Cache-Control":"no-store", "Content-Type": "application/json; charset=utf-8" }; 
}