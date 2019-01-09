const express = require('express');
const app = express();
const port = 4000 || process.env.PORT;
const syncChainToRedis = require('./app/jobs/SyncChainToRedis.js');
const syncjob = new syncChainToRedis();
const bodyParser = require('body-parser');

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }));

// parse application/json
app.use(bodyParser.json());
app.use('/', express.static('public'));

app.get('/start', (req, res) => {
  console.log("**start job**");
  syncjob.start(function (answer) {
    res.send(answer);
  })
});

app.listen(port, () => {
  // fallback - use your fallback strategy (local node / hosted node + in-dapp id mgmt / fail)
  //truffle_connect.web3 = new Web3(new Web3.providers.HttpProvider("http://localhost:8545"));
  console.log("Express Listening at http://localhost:" + port);
});