require('./mqttHandler');
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
    timestamp: new Date().toISOString()
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'Biogas MQTT Receiver is Running',
    status: 'active',
    endpoints: {
      health: '/health',
      status: '/api/status'
    }
  });
});

// Status endpoint untuk monitoring
app.get('/api/status', (req, res) => {
  res.json({
    service: 'Biogas MQTT Receiver',
    mqtt: {
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
});
