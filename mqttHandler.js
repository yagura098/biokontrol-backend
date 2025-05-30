const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
const express = require('express');
const cors = require('cors');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

// Tambahkan timeout dan logging untuk MQTT connection
const mqttOptions = {
  connectTimeout: 60000, // 60 detik
  reconnectPeriod: 1000,  // 1 detik
  keepalive: 60,
  clean: true,
  rejectUnauthorized: false // Jika menggunakan SSL self-signed
};

const client = mqtt.connect(process.env.MQTT_BROKER, mqttOptions);

// Store sensor_id untuk linking control data
let currentSensorId = null;

// Store untuk tracking kalibrasi pH
let phCalibrationStatus = {
  isCalibrating: false,
  lastCalibration: null,
  pendingCalibration: null
};

// Store untuk tracking kalibrasi pH
let phCalibrationStatus = {
  isCalibrating: false,
  lastCalibration: null,
  pendingCalibration: null
};

// Utility function untuk validasi dan sanitasi data float
function sanitizeFloatValue(value, fieldName) {
  if (value === null || value === undefined) {
    console.warn(`⚠️ ${fieldName} is null/undefined, setting to 0`);
    return 0;
  }
}

// ----- EXPRESS API ROUTES -----

// API endpoint untuk kalibrasi pH
app.post('/api/calibrate-ph', async (req, res) => {
  try {
    const { referencePh, currentPh } = req.body;
    
    // Validasi input
    if (!referencePh || !currentPh) {
      return res.status(400).json({
        success: false,
        message: 'referencePh dan currentPh diperlukan'
      });
    }
    
    const refPh = parseFloat(referencePh);
    const curPh = parseFloat(currentPh);
    
    if (isNaN(refPh) || isNaN(curPh)) {
      return res.status(400).json({
        success: false,
        message: 'Nilai pH harus berupa angka yang valid'
      });
    }
    
    // Hitung offset
    const offset = sanitizeFloatValue(refPh - curPh, 'pH offset');
    
    console.log(`🧪 pH Calibration requested:`);
    console.log(`   Reference pH: ${refPh}`);
    console.log(`   Current pH: ${curPh}`);
    console.log(`   Calculated offset: ${offset}`);
    
    // Cek apakah MQTT connected
    if (!client.connected) {
      return res.status(503).json({
        success: false,
        message: 'MQTT client tidak terhubung'
      });
    }
    
    // Cek apakah sedang dalam proses kalibrasi
    if (phCalibrationStatus.isCalibrating) {
      return res.status(409).json({
        success: false,
        message: 'Kalibrasi pH sedang dalam proses'
      });
    }
    
    // Set status kalibrasi
    phCalibrationStatus.isCalibrating = true;
    phCalibrationStatus.pendingCalibration = {
      referencePh: refPh,
      currentPh: curPh,
      offset: offset,
      timestamp: new Date().toISOString()
    };
    
    // Publish offset ke ESP32 sesuai format yang diminta
    client.publish('biogas/ph_offset', offset.toString(), { qos: 1 }, (err) => {
      if (err) {
        console.error('❌ Failed to publish pH offset:', err);
        phCalibrationStatus.isCalibrating = false;
        phCalibrationStatus.pendingCalibration = null;
        return res.status(500).json({
          success: false,
          message: 'Gagal mengirim offset ke ESP32'
        });
      }
      
      console.log(`✅ pH offset published: ${offset}`);
      
      // Response sukses
      res.json({
        success: true,
        message: 'Offset pH berhasil dikirim ke ESP32',
        data: {
          referencePh: refPh,
          currentPh: curPh,
          offset: offset,
          timestamp: new Date().toISOString()
        }
      });
    });
    
  } catch (err) {
    console.error('❌ Error in pH calibration endpoint:', err);
    phCalibrationStatus.isCalibrating = false;
    phCalibrationStatus.pendingCalibration = null;
    
    res.status(500).json({
      success: false,
      message: 'Internal server error'
    });
  }
});

// API endpoint untuk mendapatkan status kalibrasi pH
app.get('/api/calibration-status', (req, res) => {
  res.json({
    success: true,
    data: {
      isCalibrating: phCalibrationStatus.isCalibrating,
      lastCalibration: phCalibrationStatus.lastCalibration,
      pendingCalibration: phCalibrationStatus.pendingCalibration,
      mqttConnected: client.connected
    }
  });
});

// Start Express server
app.listen(PORT, () => {
  console.log(`🚀 Express server running on port ${PORT}`);
  console.log(`🧪 pH Calibration: POST http://localhost:${PORT}/api/calibrate-ph`);
  console.log(`📋 Calibration status: GET http://localhost:${PORT}/api/calibration-status`);
});
}

async function processPhCalibrationResponse(payload) {
  try {
    console.log('🧪 pH Calibration Response received:', payload);
    
    // Update status kalibrasi
    phCalibrationStatus.isCalibrating = false;
    phCalibrationStatus.lastCalibration = {
      timestamp: new Date().toISOString(),
      response: payload,
      success: payload.success || payload.status === 'success'
    };
    
    // Log hasil kalibrasi
    if (payload.success || payload.status === 'success') {
      console.log('✅ pH Calibration successful:', payload);
    } else {
      console.error('❌ pH Calibration failed:', payload);
    }
    
  } catch (err) {
    console.error('❌ Error processing pH calibration response:', err.message);
    console.error('❌ Stack trace:', err.stack);
  }
  
  const parsed = parseFloat(value);
  if (isNaN(parsed)) {
    console.warn(`⚠️ ${fieldName} is not a valid number: ${value}, setting to 0`);
    return 0;
  }
  
  // Round to 6 decimal places untuk menghindari floating point precision issues
  return Math.round(parsed * 1000000) / 1000000;
}

// Utility function untuk validasi data sensor
function validateSensorData(sensors) {
  if (!sensors || typeof sensors !== 'object') {
    throw new Error('Invalid sensors data structure');
  }
  
  return {
    ph: sanitizeFloatValue(sensors.ph, 'pH'),
    temp: sanitizeFloatValue(sensors.temp, 'temperature'),
    ch4: sanitizeFloatValue(sensors.ch4, 'CH4'),
    pressure: sanitizeFloatValue(sensors.pressure, 'pressure')
  };
}

// Utility function untuk validasi data sensor errors
function validateSensorErrors(sensorErrors) {
  if (!sensorErrors || typeof sensorErrors !== 'object') {
    throw new Error('Invalid sensor_errors data structure');
  }
  
  return {
    ph_error: sanitizeFloatValue(sensorErrors.ph_error, 'pH error'),
    ph_delta_error: sanitizeFloatValue(sensorErrors.ph_delta_error, 'pH delta error'),
    temp_error: sanitizeFloatValue(sensorErrors.temp_error, 'temperature error'),
    temp_delta_error: sanitizeFloatValue(sensorErrors.temp_delta_error, 'temperature delta error')
  };
}

// Utility function untuk validasi data actuator
function validateActuatorData(actuators) {
  if (!actuators || typeof actuators !== 'object') {
    throw new Error('Invalid actuators data structure');
  }
  
  return {
    pump_base: sanitizeFloatValue(actuators.pump_base, 'pump_base'),
    pump_acid: sanitizeFloatValue(actuators.pump_acid, 'pump_acid'),
    heater: sanitizeFloatValue(actuators.heater, 'heater'),
    solenoid: sanitizeFloatValue(actuators.solenoid, 'solenoid'),
    stirrer: sanitizeFloatValue(actuators.stirrer, 'stirrer')
  };
}

// ----- MQTT Connection -----
client.on('connect', () => {
  console.log('🟢 MQTT connected to:', process.env.MQTT_BROKER);
  console.log('🔗 Client ID:', client.options.clientId);
  
  // Subscribe dengan Promise untuk better error handling
  const subscribeToTopic = (topic) => {
    return new Promise((resolve, reject) => {
      client.subscribe(topic, { qos: 1 }, (err) => {
        if (err) {
          console.error(`❌ Failed to subscribe to ${topic}:`, err);
          reject(err);
        } else {
          console.log(`✅ Successfully subscribed to: ${topic}`);
          resolve();
        }
      });
    });
  };

  // Subscribe ke semua topic yang diperlukan
  Promise.all([
    subscribeToTopic('biogas/data/sensors'),
    subscribeToTopic('biogas/data/control'),
    subscribeToTopic('biogas/ph_calibration/response') // Topic untuk response kalibrasi pH
  ]).then(() => {
    console.log('🎯 All subscriptions completed');
  }).catch((err) => {
    console.error('❌ Subscription error:', err);
  });
});

client.on('message', async (topic, message) => {
  const messageStr = message.toString();
  
  try {
    console.log(`📥 Raw message received from ${topic}:`, messageStr);
    
    // Validasi JSON format
    let payload;
    try {
      payload = JSON.parse(messageStr);
    } catch (parseError) {
      console.error('❌ Invalid JSON format:', parseError.message);
      console.error('📄 Raw message:', messageStr);
      return;
    }
    
    console.log(`📥 Parsed payload from ${topic}:`, JSON.stringify(payload, null, 2));

    switch (topic) {
      case 'biogas/data/sensors':
        console.log('🔄 Processing sensor data...');
        await processSensorData(payload);
        break;
        
      case 'biogas/data/control':
        console.log('🔄 Processing control data...');
        await processControlData(payload);
        break;
        
      case 'biogas/ph_calibration/response':
        console.log('🔄 Processing pH calibration response...');
        await processPhCalibrationResponse(payload);
        break;
        
      default:
        console.warn('⚠️ Unknown topic:', topic);
    }
  } catch (err) {
    console.error('❌ Failed to handle MQTT message:', err.message);
    console.error('❌ Stack trace:', err.stack);
    console.error('📄 Raw message:', messageStr);
  }
});

async function processSensorData(payload) {
  try {
    if (payload.sensors && payload.sensor_errors) {
      // Payload berisi kedua data sensors dan sensor_errors
      const sensorId = await insertSensorData(payload.sensors);
      if (sensorId) {
        currentSensorId = sensorId;
        await insertSensorErrors(sensorId, payload.sensor_errors);
      }
    } else if (payload.sensors) {
      // Hanya data sensors
      const sensorId = await insertSensorData(payload.sensors);
      if (sensorId) currentSensorId = sensorId;
    } else {
      console.warn('⚠️ No sensor data found in payload');
    }
  } catch (err) {
    console.error('❌ Error processing sensor data:', err.message);
    throw err;
  }
}

async function processControlData(payload) {
  try {
    if (payload.actuators) {
      // Gunakan currentSensorId atau cari yang terbaru
      const sensorId = currentSensorId || await getLatestSensorId();
      if (sensorId) {
        await insertActuatorData(sensorId, payload.actuators);
      } else {
        console.warn('⚠️ No sensor_id available for actuator data');
      }
    } else {
      console.warn('⚠️ No actuator data found in payload');
    }
  } catch (err) {
    console.error('❌ Error processing control data:', err.message);
    throw err;
  }
}

async function processPhCalibrationResponse(payload) {
  try {
    console.log('🧪 pH Calibration Response received:', payload);
    
    // Update status kalibrasi
    phCalibrationStatus.isCalibrating = false;
    phCalibrationStatus.lastCalibration = {
      timestamp: new Date().toISOString(),
      response: payload,
      success: payload.success || payload.status === 'success'
    };
    
    // Log hasil kalibrasi
    if (payload.success || payload.status === 'success') {
      console.log('✅ pH Calibration successful:', payload);
    } else {
      console.error('❌ pH Calibration failed:', payload);
    }
    
    // Optional: Simpan log kalibrasi ke database
    await logPhCalibration(payload);
    
  } catch (err) {
    console.error('❌ Error processing pH calibration response:', err.message);
    console.error('❌ Stack trace:', err.stack);
  }
}

// Tambahkan event listeners untuk debugging
client.on('reconnect', () => {
  console.log('🔄 MQTT reconnecting...');
});

client.on('close', () => {
  console.log('🔴 MQTT connection closed');
});

client.on('disconnect', () => {
  console.log('🔴 MQTT disconnected');
});

client.on('offline', () => {
  console.log('🔴 MQTT client offline');
});

client.on('error', (err) => {
  console.error('❌ MQTT connection error:', err.message);
  console.error('❌ Error details:', err);
});

// Tambahkan periodic health check
setInterval(() => {
  if (client.connected) {
    console.log('💓 MQTT heartbeat - connected');
  } else {
    console.log('💔 MQTT heartbeat - disconnected');
  }
}, 30000); // Setiap 30 detik

async function insertSensorData(sensors) {
  try {
    console.log('💾 Inserting sensor data:', sensors);
    
    // Validasi dan sanitasi data
    const validatedSensors = validateSensorData(sensors);
    console.log('✅ Validated sensor data:', validatedSensors);
    
    const { data, error } = await supabase
      .from('sensors')
      .insert([validatedSensors])
      .select('id');

    if (error) {
      console.error('❌ Error inserting sensor data:', error);
      return null;
    }
    console.log(`✅ Inserted sensor data with id: ${data[0].id}`);
    return data[0].id;
  } catch (err) {
    console.error('❌ Exception inserting sensor data:', err.message);
    console.error('❌ Stack trace:', err.stack);
    return null;
  }
}

async function insertSensorErrors(sensorId, sensorErrors) {
  try {
    console.log('💾 Inserting sensor errors for sensor_id:', sensorId);
    
    // Validasi dan sanitasi data
    const validatedErrors = validateSensorErrors(sensorErrors);
    console.log('✅ Validated sensor errors:', validatedErrors);
    
    const { error } = await supabase
      .from('sensor_errors')
      .insert([{
        sensor_id: sensorId,
        ...validatedErrors
      }]);

    if (error) {
      console.error('❌ Error inserting sensor errors:', error);
    } else {
      console.log(`✅ Inserted sensor errors for sensor_id: ${sensorId}`);
    }
  } catch (err) {
    console.error('❌ Exception inserting sensor errors:', err.message);
    console.error('❌ Stack trace:', err.stack);
  }
}

async function insertActuatorData(sensorId, actuators) {
  try {
    // Validasi dan sanitasi data
    const validatedActuators = validateActuatorData(actuators);
    console.log('✅ Validated actuator data:', validatedActuators);
    
    const { error } = await supabase
      .from('actuators')
      .insert([{
        ...validatedActuators
      }]);

    if (error) {
      console.error('❌ Error inserting actuator data:', error);
    } else {
      console.log(`✅ Inserted actuator data for sensor_id: ${sensorId}`);
    }
  } catch (err) {
    console.error('❌ Exception inserting actuator data:', err.message);
    console.error('❌ Stack trace:', err.stack);
  }
}

async function getLatestSensorId() {
  try {
    console.log('🔍 Getting latest sensor ID...');
    const { data, error } = await supabase
      .from('sensors')
      .select('id')
      .order('created_at', { ascending: false })
      .limit(1)
      .single();

    if (error || !data) {
      console.warn('⚠️ Cannot find latest sensor_id:', error?.message);
      return null;
    }
    console.log('🆔 Latest sensor ID:', data.id);
    return data.id;
  } catch (err) {
    console.error('❌ Exception getting latest sensor_id:', err.message);
    console.error('❌ Stack trace:', err.stack);
    return null;
  }
}

// Function untuk menyimpan log kalibrasi pH
async function logPhCalibration(calibrationData) {
  try {
    const logData = {
      calibration_type: 'ph_offset',
      offset_value: phCalibrationStatus.pendingCalibration?.offset || null,
      reference_ph: phCalibrationStatus.pendingCalibration?.referencePh || null,
      current_ph: phCalibrationStatus.pendingCalibration?.currentPh || null,
      response_data: calibrationData,
      success: calibrationData.success || calibrationData.status === 'success',
      timestamp: new Date().toISOString()
    };
    
    // Jika ada tabel calibration_logs di database
    const { error } = await supabase
      .from('calibration_logs')
      .insert([logData]);
    
    if (error) {
      console.warn('⚠️ Could not save calibration log:', error.message);
    } else {
      console.log('✅ Calibration log saved successfully');
    }
  } catch (err) {
    console.warn('⚠️ Exception saving calibration log:', err.message);
  }
}

// ----- EXPRESS API ROUTES -----

// API endpoint untuk kalibrasi pH
app.post('/api/calibrate-ph', async (req, res) => {
  try {
    const { referencePh, currentPh } = req.body;
    
    // Validasi input
    if (!referencePh || !currentPh) {
      return res.status(400).json({
        success: false,
        message: 'referencePh dan currentPh diperlukan'
      });
    }
    
    const refPh = parseFloat(referencePh);
    const curPh = parseFloat(currentPh);
    
    if (isNaN(refPh) || isNaN(curPh)) {
      return res.status(400).json({
        success: false,
        message: 'Nilai pH harus berupa angka yang valid'
      });
    }
    
    // Hitung offset
    const offset = sanitizeFloatValue(refPh - curPh, 'pH offset');
    
    console.log(`🧪 pH Calibration requested:`);
    console.log(`   Reference pH: ${refPh}`);
    console.log(`   Current pH: ${curPh}`);
    console.log(`   Calculated offset: ${offset}`);
    
    // Cek apakah MQTT connected
    if (!client.connected) {
      return res.status(503).json({
        success: false,
        message: 'MQTT client tidak terhubung'
      });
    }
    
    // Cek apakah sedang dalam proses kalibrasi
    if (phCalibrationStatus.isCalibrating) {
      return res.status(409).json({
        success: false,
        message: 'Kalibrasi pH sedang dalam proses'
      });
    }
    
    // Set status kalibrasi
    phCalibrationStatus.isCalibrating = true;
    phCalibrationStatus.pendingCalibration = {
      referencePh: refPh,
      currentPh: curPh,
      offset: offset,
      timestamp: new Date().toISOString()
    };
    
    // Publish offset ke ESP32
    const calibrationPayload = {
      offset: offset,
      reference_ph: refPh,
      current_ph: curPh,
      timestamp: new Date().toISOString()
    };
    
    client.publish('biogas/ph_offset', JSON.stringify(calibrationPayload), { qos: 1 }, (err) => {
      if (err) {
        console.error('❌ Failed to publish pH offset:', err);
        phCalibrationStatus.isCalibrating = false;
        phCalibrationStatus.pendingCalibration = null;
        return res.status(500).json({
          success: false,
          message: 'Gagal mengirim offset ke ESP32'
        });
      }
      
      console.log(`✅ pH offset published: ${offset}`);
      
      // Response sukses
      res.json({
        success: true,
        message: 'Offset pH berhasil dikirim ke ESP32',
        data: {
          referencePh: refPh,
          currentPh: curPh,
          offset: offset,
          timestamp: new Date().toISOString()
        }
      });
    });
    
  } catch (err) {
    console.error('❌ Error in pH calibration endpoint:', err);
    phCalibrationStatus.isCalibrating = false;
    phCalibrationStatus.pendingCalibration = null;
    
    res.status(500).json({
      success: false,
      message: 'Internal server error'
    });
  }
});

// API endpoint untuk mendapatkan status kalibrasi pH
app.get('/api/calibration-status', (req, res) => {
  res.json({
    success: true,
    data: {
      isCalibrating: phCalibrationStatus.isCalibrating,
      lastCalibration: phCalibrationStatus.lastCalibration,
      pendingCalibration: phCalibrationStatus.pendingCalibration,
      mqttConnected: client.connected
    }
  });
});

// API endpoint untuk mendapatkan data sensor terbaru
app.get('/api/latest-sensor-data', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('sensors')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(1)
      .single();
    
    if (error) {
      return res.status(404).json({
        success: false,
        message: 'Data sensor tidak ditemukan'
      });
    }
    
    res.json({
      success: true,
      data: data
    });
  } catch (err) {
    console.error('❌ Error getting latest sensor data:', err);
    res.status(500).json({
      success: false,
      message: 'Internal server error'
    });
  }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({
    success: true,
    message: 'Server is healthy',
    mqtt_connected: client.connected,
    timestamp: new Date().toISOString()
  });
});

// Start Express server
app.listen(PORT, () => {
  console.log(`🚀 Express server running on port ${PORT}`);
  console.log(`📡 Health check: http://localhost:${PORT}/api/health`);
  console.log(`🧪 pH Calibration: POST http://localhost:${PORT}/api/calibrate-ph`);
  console.log(`📊 Latest sensor data: GET http://localhost:${PORT}/api/latest-sensor-data`);
  console.log(`📋 Calibration status: GET http://localhost:${PORT}/api/calibration-status`);
});

// ----- Graceful shutdown -----
process.on('SIGINT', () => {
  console.log('🔴 SIGINT received, shutting down...');
  client.end();
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('🔴 SIGTERM received, shutting down...');
  client.end();
  process.exit(0);
});

// Export client untuk debugging jika diperlukan
module.exports = { client, currentSensorId, app };
