const { client, isWarmupActive, publishPhOffset } = require('./mqttHandler');
const express = require('express');
const cors = require('cors')
const app = express();

// Railway akan set PORT environment variable
const port = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(cors());

// Health check endpoint untuk Railway
app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'OK', 
    service: 'Biogas MQTT Receiver',
    timestamp: new Date().toISOString(),
    mqtt_connected: client.connected,
    warmup_active: isWarmupActive
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'Biogas MQTT Receiver is Running',
    status: 'active',
    mqtt_status: client.connected ? 'connected' : 'disconnected',
    warmup_status: isWarmupActive ? 'active' : 'inactive',
    endpoints: {
      health: '/health',
      status: '/api/status',
      debug: '/debug',
      calibrate_ph: '/api/calibrate-ph'
    }
  });
});

// Debug endpoint untuk troubleshooting
app.get('/debug', (req, res) => {
  res.json({
    service: 'Biogas MQTT Receiver',
    environment: {
      NODE_ENV: process.env.NODE_ENV,
      PORT: process.env.PORT,
      mqtt_broker: process.env.MQTT_BROKER ? 'SET' : 'NOT_SET',
      supabase_url: process.env.SUPABASE_URL ? 'SET' : 'NOT_SET',
      supabase_key: process.env.SUPABASE_KEY ? 'SET' : 'NOT_SET'
    },
    mqtt: {
      connected: client.connected,
      reconnecting: client.reconnecting,
      clientId: client.options?.clientId,
      broker: process.env.MQTT_BROKER,
      topics: ['biogas/data/sensors', 'biogas/data/control']
    },
    system: {
      warmup_active: isWarmupActive
    },
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

// Status endpoint untuk monitoring
app.get('/api/status', (req, res) => {
  res.json({
    service: 'Biogas MQTT Receiver',
    mqtt: {
      connected: client.connected,
      topics: ['biogas/data/sensors', 'biogas/data/control']
    },
    system: {
      warmup_active: isWarmupActive
    },
    database: 'Supabase',
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

// pH Calibration endpoint
app.post('/api/calibrate-ph', async (req, res) => {
  try {
    const { referencePh, currentPh } = req.body;
    
    // Validasi input
    if (!referencePh || !currentPh) {
      return res.status(400).json({
        success: false,
        message: 'Mohon isi kedua nilai pH (referencePh dan currentPh)'
      });
    }
    
    const refPh = parseFloat(referencePh);
    const curPh = parseFloat(currentPh);
    
    // Validasi apakah nilai adalah angka yang valid
    if (isNaN(refPh) || isNaN(curPh)) {
      return res.status(400).json({
        success: false,
        message: 'Nilai pH harus berupa angka yang valid'
      });
    }
    
    // Validasi rentang pH (0-14)
    // if (refPh < 0 || refPh > 14 || curPh < 0 || curPh > 14) {
    //   return res.status(400).json({
    //     success: false,
    //     message: 'Nilai pH harus berada dalam rentang 0-14'
    //   });
    // }
    
    // Hitung offset: offset = reference - current
    const offset = refPh - curPh;
    
    // Validasi bahwa MQTT client terhubung
    if (!client.connected) {
      return res.status(503).json({
        success: false,
        message: 'MQTT client tidak terhubung. Tidak dapat mengirim offset pH.'
      });
    }
    
    // Kirim offset via MQTT
    try {
      publishPhOffset(offset);
      
      console.log(`🧪 pH Calibration completed:`, {
        referencePh: refPh,
        currentPh: curPh,
        offset: offset
      });
      
      res.json({
        success: true,
        message: 'Kalibrasi pH berhasil dikirim',
        data: {
          referencePh: refPh,
          currentPh: curPh,
          offset: offset,
          timestamp: new Date().toISOString()
        }
      });
      
    } catch (mqttError) {
      console.error('❌ Error publishing pH offset:', mqttError);
      res.status(500).json({
        success: false,
        message: 'Gagal mengirim offset pH via MQTT: ' + mqttError.message
      });
    }
    
  } catch (error) {
    console.error('❌ Error in pH calibration:', error);
    res.status(500).json({
      success: false,
      message: 'Terjadi kesalahan internal saat kalibrasi pH: ' + error.message
    });
  }
});

// Endpoint untuk mendapatkan status warmup saja
app.get('/api/warmup-status', (req, res) => {
  res.json({
    warmup_active: isWarmupActive,
    timestamp: new Date().toISOString()
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('❌ Unhandled error:', err);
  res.status(500).json({
    error: 'Internal server error',
    timestamp: new Date().toISOString()
  });
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    error: 'Endpoint not found',
    timestamp: new Date().toISOString()
  });
});

// Bind to 0.0.0.0 untuk Railway
app.listen(port, '0.0.0.0', () => {
  console.log(`🚀 Server listening at http://0.0.0.0:${port}`);
  console.log(`📡 MQTT Handler initialized`);
  console.log(`🗄️ Connected to Supabase`);
  console.log(`🔍 Debug endpoint available at: /debug`);
  console.log(`🧪 pH Calibration endpoint available at: /api/calibrate-ph`);
  
  // Log environment info (tanpa sensitive data)
  console.log('🌍 Environment check:');
  console.log('  - MQTT_BROKER:', process.env.MQTT_BROKER ? 'SET' : 'NOT_SET');
  console.log('  - SUPABASE_URL:', process.env.SUPABASE_URL ? 'SET' : 'NOT_SET');
  console.log('  - SUPABASE_KEY:', process.env.SUPABASE_KEY ? 'SET' : 'NOT_SET');
});
