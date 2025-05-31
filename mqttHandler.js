const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

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
// Store warmup status
let isWarmupActive = false;

// Utility function untuk validasi dan sanitasi data float
function sanitizeFloatValue(value, fieldName) {
  if (value === null || value === undefined) {
    console.warn(`⚠️ ${fieldName} is null/undefined, setting to 0`);
    return 0;
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

// Function untuk mengirim pH offset via MQTT
function publishPhOffset(offset) {
  const payload = {
    ph_offset: offset,
    timestamp: new Date().toISOString()
  };
  
  client.publish('biogas/ph_offset', JSON.stringify(payload), { qos: 1 }, (err) => {
    if (err) {
      console.error('❌ Failed to publish pH offset:', err);
    } else {
      console.log(`✅ Published pH offset: ${offset} to biogas/ph_offset`);
    }
  });
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

  // Subscribe ke kedua topic
  Promise.all([
    subscribeToTopic('biogas/data/sensors'),
    subscribeToTopic('biogas/data/control')
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
    // Cek warmup status sebelum menyimpan data
    if (isWarmupActive) {
      console.log('🔄 Warmup active - skipping sensor data storage');
      return;
    }

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
    // Update warmup status dari system data
    if (payload.system && payload.system.warmup_active !== undefined) {
      const newWarmupStatus = payload.system.warmup_active === 1;
      if (newWarmupStatus !== isWarmupActive) {
        isWarmupActive = newWarmupStatus;
        console.log(`🔄 Warmup status changed to: ${isWarmupActive ? 'ACTIVE' : 'INACTIVE'}`);
      }
    }

    // Cek warmup status sebelum menyimpan data actuator
    if (isWarmupActive) {
      console.log('🔄 Warmup active - skipping actuator data storage');
      return;
    }

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

// ----- Graceful shutdown -----
process.on('SIGINT', () => {
  console.log('🔴 SIGINT received, shutting down MQTT client...');
  client.end();
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('🔴 SIGTERM received, shutting down MQTT client...');
  client.end();
  process.exit(0);
});

// Export client dan functions untuk debugging jika diperlukan
module.exports = { 
  client, 
  currentSensorId, 
  isWarmupActive,
  publishPhOffset 
};