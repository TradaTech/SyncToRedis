// const Web3                        = require('web3');
require('dotenv').config();
const _                           = require('lodash');
const async                       = require('async');
const Utils                       = require('../common/Utils');
const redis                       = require('redis');

const bnbABI                      = require('../../config/abi/bnb');
const memoryABI                   = require('../../config/abi/propose');
const proposeABI                  = require('../../config/abi/memory');
const network                     = require('../../config/network');

const web3                        = Utils.getWeb3Instance();
const client                      = redis.createClient();
const importMulti                 = client.multi();
const bnbContract                 = new web3.eth.Contract(bnbABI, network.contractAddresses.bnb);
const proposeContract             = new web3.eth.Contract(proposeABI, network.contractAddresses.propose);
const memoryContract              = new web3.eth.Contract(memoryABI, network.contractAddresses.memory);

let LATEST_PROCESSED_BLOCK  = 0;
// const env                   = process.env;
const BATCH_BLOCK_SIZE      = parseInt(process.env.BATCH_BLOCK_SIZE || 500);
const REQUIRED_CONFIRMATION = parseInt(process.env.REQUIRED_CONFIRMATION || 7);
const PARALLEL_INSERT_LIMIT = 1000;
const DB_PROPOSE_INDEX      = parseInt(process.env.DB_PROPOSE_INDEX || 0);
const DB_MEMORY_INDEX       = parseInt(process.env.DB_MEMORY_INDEX || 1);

//For test
var countLogs, start, millis,start0, millis0,start1, millis1,start2, millis2;


class SyncChainToRedis {

  redisconnect() {
    //Handle event redis
    client.on('connect', function() {
      console.log('Redis client connected');
    });
    client.on('error', function(err){
      console.log('Something went wrong ', err)
    });
  }

  start () {
    console.log("Web3 version: ",web3.version);
    this.redisconnect();
    async.auto({
      latestProcessedBlock: (next) => {
        if (LATEST_PROCESSED_BLOCK > 0) {
          return next(null, LATEST_PROCESSED_BLOCK);
        }

        this.getLatestBlockNumber(next);
      },
      processBlocks: ['latestProcessedBlock', (ret, next) => {
        this.processBlocks(ret.latestProcessedBlock, next);
      }]
    }, (err, ret) => {
      let timer = network.averageBlockTime;
      if (err) {
        console.log(err);
        timer = 5000;
        console.log(`SyncJob will be restart in ${timer} ms...`);
      } else {
        console.log(`Already processed the newest block. SyncJob will be restarted in 1 block...`);
      }
      setTimeout(() => {
        this.start();
      }, timer);
    });
  }

  //Get lates block number form redis
  getLatestBlockNumber(next) {
   return next(null,process.env.START_BLOCKNUMBER);
  }

  //
  processBlocks (latestProcessedBlock, callback) {
    start = Date.now();
    let fromBlockNumber, toBlockNumber;
    latestProcessedBlock  = parseInt(latestProcessedBlock);

    async.auto({
      latestOnchainBlock: (next) => {
        web3.eth.getBlockNumber(next);
      },
      processBlocksOnce: ['latestOnchainBlock', (ret, next) => {
        const latestOnchainBlock = ret.latestOnchainBlock;
        console.log("****** latestOnchainBlock ******:= ",latestOnchainBlock);
        fromBlockNumber = latestProcessedBlock;

        // Crawl the newest block already
        if (fromBlockNumber > latestOnchainBlock - REQUIRED_CONFIRMATION) {
          toBlockNumber = latestProcessedBlock;
          return next(null, true);
        }

        toBlockNumber = latestProcessedBlock + BATCH_BLOCK_SIZE;
        if (toBlockNumber > latestOnchainBlock - REQUIRED_CONFIRMATION) {
          toBlockNumber = latestOnchainBlock - REQUIRED_CONFIRMATION;
        }

        if (toBlockNumber <= fromBlockNumber) {
          return next(null, true);
        }
        
        this._processBlocksOnce(fromBlockNumber, toBlockNumber, next);
      }]
    }, (err, ret) => {
      if (err) {
        return callback(err);
      }

      if (ret.processBlocksOnce === true) {
        return callback(null, true);
      }

      LATEST_PROCESSED_BLOCK = toBlockNumber;
      process.nextTick(() => {
        this.processBlocks(LATEST_PROCESSED_BLOCK, callback);
      });
    });
  }

  //
  _processBlocksOnce (fromBlockNumber, toBlockNumber, callback) {
    console.log(`_processBlocksOnce: ${fromBlockNumber} → ${toBlockNumber}`);
    async.auto({
      logs: (next) => {
        this.getTransfer(fromBlockNumber, toBlockNumber, next);
      },
      // proprose: (next) => {
      //   this.getProprose(fromBlockNumber, toBlockNumber, next);
      // },
      // memory: (next) => {
      //   this.getMemory(fromBlockNumber, toBlockNumber, next);
      // },
      blockTimestamps: ['logs', (ret, next) => {
        this.getBlockTimestamp(ret,next);
      }],
      processLogs: ['blockTimestamps', (ret, next) => {
        this._processLogData(ret.logs, ret.blockTimestamps, next);
      }],
      // processProprose: ['blockTimestamps', 'proprose', (ret, next) => {
      //   this._processProposeData(ret.proprose, ret.blockTimestamps, next);
      // }],
      // processMemory: ['blockTimestamps', 'memory', (ret, next) => {
      //   this._processMemoryData(ret.memory, ret.blockTimestamps, next);
      // }],
      // savedata:[ 'processLogs','processProprose','processMemory', (ret, next) =>{
      //   this._saveToRds(ret.processData, next);
      // }],
      savedata:[ 'processLogs', (ret, next) =>{
        this._saveToRds(ret.processLogs, next);
      }]
    }, callback);
  }
  //for test bnb
  getTransfer(fromBlockNumber, toBlockNumber, callback) {
    start0 = Date.now();
    bnbContract.getPastEvents("Transfer", {
      //filter: {myIndexedParam: [20,23], myOtherIndexedParam: '0x123456789...'}, // Using an array means OR: e.g. 20 or 23
      fromBlock: fromBlockNumber,
      toBlock: toBlockNumber
    }, (err, events) => {
      if (err) {
        return callback(`Cannot query data from network: ${err.toString()}`);
      }
      millis0 = Date.now() - start0;
      console.log("logs:= ",events.length);
      console.log("  Time getLogs: ",millis0/1000);
      countLogs  = events.length;
      return callback(null, events);
    })
    .then(function(events){
        //console.log(events) // same results as the optional callback above
    });
  }
  //for locklove
  getProprose(fromBlockNumber, toBlockNumber, callback) {
    console.log("getProprose");
    callback(null,"getProprose");
  }
  //for locklove
  getMemory(fromBlockNumber, toBlockNumber, callback) {
    console.log("getMemory");
    callback(null,"getMemory");
  }
  //
  getBlockTimestamp (ret, callback) {
    start1 = Date.now();
    const blockNumbers = _.map(ret.logs, 'blockNumber');
    const blockTimestamps = {};

    async.each(blockNumbers, (blockNumber, _next) => {
      web3.eth.getBlock(blockNumber, (_err, block) => {
        if (_err) {
          return _next(_err);
        }
        blockTimestamps[blockNumber] = block.timestamp;
        _next(null, null);
      });
    }, (_err) => {
      if (_err) {
        return next(_err);
      }
      millis1 = Date.now() - start1;
      console.log("  Time blockTimestamps: ",millis1/1000);
      return callback(null, blockTimestamps);
    });
  }
  //
  _processProposeData (proposes, blockTimestamps, callback) {
    console.log("_processProposeData: ",proposes);
    const records = {};
    _.each(proposes, (item) => {
      const txid = item.transactionHash;
      if (!records[txid]) {
        records[txid] = {};
      }
      const timestamp = blockTimestamps[item.blockNumber];
      if (!timestamp) {
        return next(`Cannot get block info for log id=${item.id}, tx=${item.transactionHash}`);
      }
      const record = records[txid];
      record.fAddress     = item.fAddress;
      record.fPropose     = item.fPropose;
      record.fImg         = item.fImg;
      record.tAddress     = item.tAddress;
      // record.tPropose     = item.tPropose;
      record.tImg         = item.tImg;
      let place = "{"+"name:"+item.place+", "+ "longitude:"+item.long+", "+"latitude:"+item.lat+"}";
      record.place        = place;
      record.createtime   = timestamp;
      record.replytime    = timestamp;
    });
    callback(null,records);
  }
  //
  _processMemoryData (memories, blockTimestamps, callback) {
    console.log("_processMemoryData: ",memories);
    const records = {};
    _.each(proposes, (item) => {
      const txid = item.transactionHash;
      if (!records[txid]) {
        records[txid] = {};
      }
      const timestamp = blockTimestamps[item.blockNumber];
      if (!timestamp) {
        return next(`Cannot get block info for log id=${item.id}, tx=${item.transactionHash}`);
      }
      const record = records[txid];
      record.proposeID    = item.proposeID;
      record.fAddress     = item.fAddress;
      record.comment      = item.comment;
      record.tImg         = item.tImg;
      let place = "{"+"name:"+item.place+", "+ "longitude:"+item.long+", "+"latitude:"+item.lat+"}";
      record.place        = place;
      record.createtime   = timestamp;
    });
    callback(null,records);
  }
  //
  _processLogData (logs, blockTimestamps, callback) {
    start2 = Date.now();
    const records = {};
    _.each(logs, (log) => {
      const txid = log.transactionHash;
      if (!records[txid]) {
        records[txid] = {};
      }
      const timestamp = blockTimestamps[log.blockNumber];
      if (!timestamp) {
        return next(`Cannot get block info for log id=${log.id}, tx=${log.transactionHash}`);
      }
      const record = records[txid];
      record.blockNumber = log.blockNumber;
      record.blockHash = log.blockHash;
      record.blockTimestamp = timestamp;
      record.tx = log.transactionHash;
      let place = "{"+"name:"+"HN"+", "+ "longitude:"+"item.long"+", "+"latitude:"+"item.lat"+"}";
      record.place        = place;
    });
    console.log("_processLogData done");
    callback(null,records);
  }

  //Test
  _saveToRds (records, callback) {
    client.select(DB_MEMORY_INDEX, function() { console.log("Select DB 1"); });
    async.waterfall([
      (next) => {
        let count =0;
        async.eachLimit(_.values(records), PARALLEL_INSERT_LIMIT, (record, _next) => {
          // this._sync_reply_propose(count,record, _next);
          importMulti.hmset(record.blockNumber,record);
          _next(null,null);
        }, next);
      },
      (next) => {
        importMulti.exec(function(err,results){
          if (err) {
            next(err);
          } else {
            //console.log(results);
            //client.quit();
            next(null,null);
           }
        });
      }
    ], (err, ret) => {
      // exSession.destroy();
      if (err) {
        return callback(err);
      }
      millis2 = Date.now() - start2;
      console.log("  Time processLogData: ",millis2/1000);

      millis = Date.now() - start;
      console.log("Total Time: ",millis/1000, " - Per log/s:",countLogs / Math.floor(millis/1000));
      console.log("---------------------------------End---------------------------------");
      return callback(null, true);
    });
  }

  //
  sync_new_propose(value) {
    client.select(DB_PROPOSE_INDEX, function() { console.log("Select DB 0"); });
    let place = "{"+"name:"+value.place+", "+ "longitude:"+value.long+", "+"latitude:"+value.lat+"}";
    client.hset(value.index, "place", place, redis.print);
    client.hset(value.index, "tAddress", value.tAddress, redis.print);
    client.hset(value.index, "fImg", value.fImg, redis.print);
    client.hset(value.index, "fPropose", value.fPropose, redis.print);
    client.hset(value.index, "fAddress", value.fAddress, redis.print);
    let timestamp =  new Date().getTime();
    client.hset(value.index, "updatetime", timestamp, redis.print);
    client.hset(value.index, "createtime", timestamp, redis.print);
    console.log("sync_data");
  }

  //
  sync_reply_propose(value) {
    client.select(DB_PROPOSE_INDEX, function() { console.log("Select DB 0"); });
    client.hset(value.index, "tImg", value.tImg, redis.print);
    client.hset(value.index, "tPropose", value.tPropose, redis.print);
    let timestamp =  new Date().getTime();
    client.hset(value.index, "updatetime", timestamp, redis.print);
    console.log("sync_data");
  }
  //
  sync_new_memory(value) {
    client.select(DB_MEMORY_INDEX, function() { console.log("Select DB 1"); });
    client.hset(value.index, "proposeID", value.proposeID, redis.print);
    client.hset(value.index, "fAddress", value.fAddress, redis.print);
    client.hset(value.index, "comment", value.comment, redis.print);
    let place = "{"+"name:"+value.place+", "+ "longitude:"+value.long+", "+"latitude:"+value.lat+"}";
    client.hset(value.index, "place", place, redis.print);
    let timestamp =  new Date().getTime();
    client.hset(value.index, "createtime", timestamp, redis.print);
    console.log("sync_data");
  }
}

// function rk(...args) {
//   return Array.prototype.slice.call(args).join(':');
// }

module.exports = SyncChainToRedis;
