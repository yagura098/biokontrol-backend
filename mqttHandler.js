const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const client = mqtt.connect(process.env.MQTT_BROKER);

// Format payload yang diterima:
// Payload 1: Sensor data
/*
{
  "sensors": {
    "ph": 6.8,
    "temp": 30.2,
    "ch4": 5042,
    "pressure": 1015.0
  }
}
*/

// Payload 2: Control data
/*
{
  "sensor_errors": {
    "ph_error": 0.5,
    "ph_delta_error": 0.1,
    "temp_error": 2.0,
    "temp_delta_error": 0.5
  },
  "actuators": {
    "pump_base": 128,
    "pump_acid": 0,
    "heater": 255,
    "solenoid": 1,
    "stirrer": 1
  }
}
*/

client.on('connect', () => {
  console.log('🟢 MQTT connected');
  client.subscribe('biogas/data');
});

client.on('message', async (topic, message) => {
  try {
    const payload = JSON.parse(message.toString());
    console.log('📥 Received payload:', JSON.stringify(payload, null, 2));

    // Cek jenis payload berdasarkan struktur data
    if (payload.sensors) {
      // Payload 1: Data sensor
      await handleSensorData(payload);
    } else if (payload.sensor_errors && payload.actuators) {
      // Payload 2: Data kontrol (error + aktuator)
      await handleControlData(payload);
    } else {
      console.warn('⚠️ Unknown payload format:', payload);
    }

  } catch (err) {
    console.error('❌ Failed to handle MQTT message:', err.message);
  }
});

async function handleSensorData(payload) {
  try {
    // Simpan data sensor
    const { data: sensorData, error: sensorError } = await supabase
      .from('sensors')
      .insert({
        ph: payload.sensors.ph,
        temp: payload.sensors.temp,
        ch4: payload.sensors.ch4,
        pressure: payload.sensors.pressure
      })
      .select()
      .single();

    if (sensorError) {
      console.error('❌ Error inserting sensor data:', sensorError);
    } else {
      console.log('✅ Sensor data saved to Supabase:', sensorData.id);
    }

  } catch (err) {
    console.error('❌ Failed to handle sensor data:', err.message);
  }
}

async function handleControlData(payload) {
  try {
    // Ambil sensor_id terbaru untuk referensi sensor_errors
    const { data: latestSensor, error: sensorQueryError } = await supabase
      .from('sensors')
      .select('id')
      .order('created_at', { ascending: false })
      .limit(1)
      .single();

    if (sensorQueryError) {
      console.error('❌ Error getting latest sensor ID:', sensorQueryError);
      return;
    }

    // Simpan data sensor errors
    const { data: errorData, error: errorError } = await supabase
      .from('sensor_errors')
      .insert({
        sensor_id: latestSensor.id,
        ph_error: payload.sensor_errors.ph_error,
        ph_delta_error: payload.sensor_errors.ph_delta_error,
        temp_error: payload.sensor_errors.temp_error,
        temp_delta_error: payload.sensor_errors.temp_delta_error
      });

    // Simpan data aktuator
    const { data: actuatorData, error: actuatorError } = await supabase
      .from('actuators')
      .insert({
        pump_base: payload.actuators.pump_base,
        pump_acid: payload.actuators.pump_acid,
        heater: payload.actuators.heater,
        solenoid: payload.actuators.solenoid,
        stirrer: payload.actuators.stirrer
      });

    if (errorError || actuatorError) {
      console.error('❌ Error inserting control data:', errorError || actuatorError);
    } else {
      console.log('✅ Control data (errors + actuators) saved to Supabase');
    }

  } catch (err) {
    console.error('❌ Failed to handle control data:', err.message);
  }
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('🔴 Shutting down MQTT client...');
  client.end();
  process.exit(0);
});

client.on('error', (err) => {
  console.error('❌ MQTT connection error:', err);
});

client.on('close', () => {
  console.log('🔴 MQTT connection closed');
});

client.on('reconnect', () => {
  console.log('🔄 MQTT reconnecting...');
});