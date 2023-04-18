require('dotenv').config();
const express = require('express');
const app = express();
const port = 3000;

var cors = require('cors');

const webhook_server = require('./stake_server');

app.use(cors());
app.use(express.json());

app.get('/test', (req, res) => {
  webhook_server.test()
  .then(response => {
    res.status(200).send(response);
  })
  .catch(error => {
    res.status(500).send(error);
  })
});

app.post('/webhooks', (req, res) => {
  webhook_server.insertParsedTransaction(req)
  .then(response => {
    res.status(200).send(response);
  })
  .catch(error => {
    res.status(500).send(error);
  })
});

app.listen(port, () => {
  console.log(`App running on port ${port}.`)
});
