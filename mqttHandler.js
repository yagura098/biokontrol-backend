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
  try {
    console.log(`📥 Raw message received from ${topic}:`, message.toString());
    const payload = JSON.parse(message.toString());
    console.log(`📥 Parsed payload from ${topic}:`, JSON.stringify(payload, null, 2));

    switch (topic) {
      case 'biogas/data/sensors':
        console.log('🔄 Processing sensor data...');
        if (payload.sensors && payload.sensor_errors) {
          // Payload sensors berisi kedua data sensors dan sensor_errors
          const sensorId = await insertSensorData(payload.sensors);
          if (sensorId) {
            currentSensorId = sensorId;
            await insertSensorErrors(sensorId, payload.sensor_errors);
          }
        } else if (payload.sensors) {
          // Hanya data sensors
          const sensorId = await insertSensorData(payload.sensors);
          if (sensorId) currentSensorId = sensorId;
        }
        break;
        
      case 'biogas/data/control':
        console.log('🔄 Processing control data...');
        if (payload.actuators) {
          // Gunakan currentSensorId atau cari yang terbaru
          const sensorId = currentSensorId || await getLatestSensorId();
          if (sensorId) {
            await insertActuatorData(sensorId, payload.actuators);
          }
        }
        break;
        
      default:
        console.warn('⚠️ Unknown topic:', topic);
    }
  } catch (err) {
    console.error('❌ Failed to handle MQTT message:', err.message);
    console.error('❌ Stack trace:', err.stack);
    console.error('📄 Raw message:', message.toString());
  }
});

// Tambahkan event listeners untuk debugging
client.on('connect', () => {
  console.log('🟢 MQTT connect event fired');
});

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
    const { data, error } = await supabase
      .from('sensors')
      .insert([{
        ph: sensors.ph,
        temp: sensors.temp,
        ch4: sensors.ch4,
        pressure: sensors.pressure,
      }])
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
    const { error } = await supabase
      .from('sensor_errors')
      .insert([{
        sensor_id: sensorId,
        ph_error: sensorErrors.ph_error,
        ph_delta_error: sensorErrors.ph_delta_error,
        temp_error: sensorErrors.temp_error,
        temp_delta_error: sensorErrors.temp_delta_error,
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
    console.log('💾 Inserting actuator data for sensor_id:', sensorId);
    const { error } = await supabase
      .from('actuators')
      .insert([{
        pump_base: actuators.pump_base,
        pump_acid: actuators.pump_acid,
        heater: actuators.heater,
        solenoid: actuators.solenoid,
        stirrer: actuators.stirrer,
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

// Export client untuk debugging jika diperlukan
module.exports = { client, currentSensorId };
