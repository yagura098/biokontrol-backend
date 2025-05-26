require('./mqttHandler');
const express = require('express');
const app = express();
const port = 3000;

app.get('/', (_, res) => {
  res.send('Biogas MQTT Receiver is Running');
});

app.listen(port, () => {
  console.log(`🚀 Server listening at http://localhost:${port}`);
});
