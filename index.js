const { client } = require('./mqttHandler');
const express = require('express');
const app = express();

// Railway akan set PORT environment variable
const port = process.env.PORT || 3000;

// Middleware
app.use(express.json());

// Health check endpoint untuk Railway
app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'OK', 
    service: 'Biogas MQTT Receiver',
    timestamp: new Date().toISOString(),
    mqtt_connected: client.connected
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'Biogas MQTT Receiver is Running',
    status: 'active',
    mqtt_status: client.connected ? 'connected' : 'disconnected',
    endpoints: {
      health: '/health',
      status: '/api/status',
      debug: '/debug'
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
    database: 'Supabase',
    uptime: process.uptime(),
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
  
  // Log environment info (tanpa sensitive data)
  console.log('🌍 Environment check:');
  console.log('  - MQTT_BROKER:', process.env.MQTT_BROKER ? 'SET' : 'NOT_SET');
  console.log('  - SUPABASE_URL:', process.env.SUPABASE_URL ? 'SET' : 'NOT_SET');
  console.log('  - SUPABASE_KEY:', process.env.SUPABASE_KEY ? 'SET' : 'NOT_SET');
});
