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

    // raw latest - all sensors
    if (path === "/weatherstation/latest") {
      const j = await env.WEATHER_KV.get("latest.json");
      console.log("read_latest", { found: !!j, len: j?.length || 0 });
      if (!j) return new Response("not found", { status: 404, headers: noStoreJson(req) });
      return new Response(j, { status: 200, headers: noStoreJson(req) });
    }

    // individual sensor data
    if (path.startsWith("/weatherstation/sensor/")) {
      const sensorId = path.split('/').pop(); // Get the sensor ID from URL
      const j = await env.WEATHER_KV.get(`sensor_${sensorId}.json`);
      console.log("read_sensor", { sensorId, found: !!j });
      if (!j) return new Response("sensor not found", { status: 404, headers: noStoreJson(req) });
      return new Response(j, { status: 200, headers: noStoreJson(req) });
    }

    return new Response("not found", { status: 404, headers: noStore(req) });
  },

  async scheduled(event, env, ctx) {
    console.log("Fetching data from Ecowitt API for all sensors (5-minute interval)...");
    
    // Define your 5 sensors with their MAC addresses and names
    const sensors = [
      { 
        id: "AW001", 
        mac: "EC:64:C9:F2:AC:79", 
        name: "Kessel-Lo Main Station",
        csvFile: "AW001.csv"
      },
      { 
        id: "AW002", 
        mac: "CC:7B:5C:51:58:E1", 
        name: "Sensor Location 2",
        csvFile: "AW002.csv"
      },
      { 
        id: "AW003", 
        mac: "24:D7:EB:EA:E5:EC", 
        name: "Sensor Location 3", 
        csvFile: "AW003.csv"
      },
      { 
        id: "AW004", 
        mac: "EC:64:C9:F1:CC:74", 
        name: "Sensor Location 4",
        csvFile: "AW004.csv"
      },
      { 
        id: "AW005", 
        mac: "CC:7B:5C:51:55:E9", 
        name: "Sensor Location 5",
        csvFile: "AW005.csv"
      }
    ];

    try {
      // Fetch data for all sensors in parallel
      const sensorPromises = sensors.map(async (sensor) => {
        try {
          const apiUrl = `https://api.ecowitt.net/api/v3/device/real_time?application_key=31B06CAD6518B81F808312D91B55973A&api_key=be6a3fde-4a04-40e0-8452-49ef32af65a0&mac=${sensor.mac}&call_back=all&temp_unitid=2&pressure_unitid=4&wind_speed_unitid=9&rainfall_unitid=13`;
          
          console.log(`Fetching data for sensor ${sensor.id} (${sensor.mac})...`);
          const response = await fetch(apiUrl);
          const data = await response.json();
          
          console.log(`Sensor ${sensor.id} API response:`, { code: data.code, msg: data.msg });
          
          if (data.code === 0) {
            const timestamp = new Date().toISOString();
            
            // Create sensor payload
            const payload = JSON.stringify({
              sensor_id: sensor.id,
              sensor_name: sensor.name,
              ecowitt_api: data.data,
              received_at: timestamp,
              api_timestamp: data.time
            }, null, 2);
            
            // Store individual sensor data
            await env.WEATHER_KV.put(`sensor_${sensor.id}.json`, payload);
            
            // Append to individual CSV file
            await appendToGitHubCSV(data.data, timestamp, env, sensor.csvFile, sensor.id);
            
            console.log(`Sensor ${sensor.id} data stored successfully`, { 
              outdoor_temp: data.data.outdoor?.temperature?.value,
              humidity: data.data.outdoor?.humidity?.value,
              timestamp: timestamp
            });
            
            return { sensorId: sensor.id, success: true, data: payload };
          } else {
            console.log(`Sensor ${sensor.id} API error:`, data.msg, data.code);
            return { sensorId: sensor.id, success: false, error: data.msg };
          }
        } catch (error) {
          console.log(`Sensor ${sensor.id} fetch failed:`, error.message);
          return { sensorId: sensor.id, success: false, error: error.message };
        }
      });

      // Wait for all sensors to complete
      const results = await Promise.all(sensorPromises);
      
      // Create combined latest data
      const combinedData = {
        timestamp: new Date().toISOString(),
        sensors: results.filter(r => r.success).map(r => ({
          sensor_id: r.sensorId,
          data: JSON.parse(r.data)
        }))
      };
      
      // Store combined data
      await env.WEATHER_KV.put("latest.json", JSON.stringify(combinedData, null, 2));
      
      // Log summary
      const successful = results.filter(r => r.success).length;
      const failed = results.filter(r => !r.success).length;
      console.log(`Scheduled job completed: ${successful} successful, ${failed} failed`);
      
    } catch (error) {
      console.log("Scheduled job failed:", error.message);
    }
  }
};

// Updated function to append data to specific CSV files
async function appendToGitHubCSV(weatherData, timestamp, env, csvFileName, sensorId) {
  try {
    console.log(`=== GITHUB CSV DEBUG START for ${sensorId} ===`);
    
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
    
    console.log("GitHub Config:", {
      sensorId: sensorId,
      csvFile: csvFileName,
      repo: repo,
      fullUrl: csvUrl
    });
    
    const headers = {
      'Authorization': `token ${env.GITHUB_TOKEN}`,
      'User-Agent': 'Cloudflare-Worker',
      'Accept': 'application/vnd.github.v3+json'
    };
    
    let existingContent = '';
    let sha = null;
    
    // Check if CSV file exists
    console.log(`Checking if ${csvFileName} exists...`);
    const fileResponse = await fetch(csvUrl, { headers });
    console.log("File check - Status:", fileResponse.status);
    
    if (fileResponse.ok) {
      const fileData = await fileResponse.json();
      existingContent = atob(fileData.content);
      sha = fileData.sha;
      console.log(`✅ Found existing CSV file for ${sensorId}, rows:`, existingContent.split('\n').length - 1);
    } else if (fileResponse.status === 404) {
      console.log(`📝 CSV file ${csvFileName} doesn't exist, will create new file`);
    } else {
      const errorText = await fileResponse.text();
      console.log(`❌ File check error for ${sensorId}:`, fileResponse.status, errorText);
      return;
    }
    
    // Create CSV header if file doesn't exist
    if (!existingContent) {
      existingContent = 'timestamp,temperature_f,humidity_pct,dew_point_f,feels_like_f,wind_speed_mph,wind_gust_mph,wind_direction_deg,pressure_inhg,rain_rate_inhr,daily_rain_in,solar_wm2,uv_index,indoor_temp_f,indoor_humidity_pct\n';
      console.log(`📄 Created CSV headers for ${sensorId}`);
    }
    
    // Append new row
    const newContent = existingContent + csvRow + '\n';
    
    console.log(`Updating file ${csvFileName} on GitHub...`);
    const updateResponse = await fetch(csvUrl, {
      method: 'PUT',
      headers,
      body: JSON.stringify({
        message: `Add 5-minute weather data for ${sensorId}: ${timestamp}`,
        content: btoa(newContent),
        sha: sha
      })
    });
    
    console.log(`GitHub update response for ${sensorId} - Status:`, updateResponse.status);
    
    if (updateResponse.ok) {
      console.log(`✅ CSV data for ${sensorId} appended to GitHub successfully`);
    } else {
      const error = await updateResponse.text();
      console.log(`❌ Failed to update GitHub CSV for ${sensorId}. Status:`, updateResponse.status, "Error:", error);
    }
    
    console.log(`=== GITHUB CSV DEBUG END for ${sensorId} ===`);
    
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