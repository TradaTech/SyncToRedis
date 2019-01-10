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
const BATCH_BLOCK_SIZE      = parseInt(process.env.BATCH_BLOCK_SIZE || 300);
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
      proprose: (next) => {
        this.getProprose(fromBlockNumber, toBlockNumber, next);
      },
      replyProprose: (next) => {
        this.getReplyProprose(fromBlockNumber, toBlockNumber, next);
      },
      memory: (next) => {
        this.getMemory(fromBlockNumber, toBlockNumber, next);
      },
      blockTimestamps: ['logs','proprose','replyProprose','memory', (ret, next) => {
        this.getBlockTimestamp(ret, next);
      }],
      processLogs: ['blockTimestamps', (ret, next) => {
        this._processLogData(ret.logs, ret.blockTimestamps, next);
      }],
      // processProprose: ['blockTimestamps', 'proprose', (ret, next) => {
      //   this._processProposeData(ret.proprose, ret.blockTimestamps, next);
      // }],
      // processReplyProprose: ['blockTimestamps', 'proprose', (ret, next) => {
      //   this._processReplyProposeData(ret.proprose, ret.blockTimestamps, next);
      // }],      
      // processMemory: ['blockTimestamps', 'memory', (ret, next) => {
      //   this._processMemoryData(ret.memory, ret.blockTimestamps, next);
      // }],
      // savedata:[ 'processLogs','processProprose','processMemory', (ret, next) =>{
      //   this._saveToRds(ret.processData, next);
      // }],
      savedata:[ 'processLogs', (ret, next) =>{
        this._saveToRds(ret.processLogs, ret.processLogs, ret.processLogs, next);
        // this._saveToRds2(ret.processLogs, next);
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
  //for test bnb
  // getTransferbk(fromBlockNumber, toBlockNumber, callback) {
  //   start0 = Date.now();
  //   const options = {
  //     fromBlock: fromBlockNumber-1, 
  //     toBlock: toBlockNumber,
  //     address: ['0x56719fDa968D45042E6761DE23c4139127315EBE']
  //   }
  //   var subscription = web3.eth.subscribe('logs', options, function(error, result){
  //     if (!error){
  //       console.log("subscription:=",result);
  //       web3.eth.clearSubscriptions();
  //     }else{
  //       console.log(error);
  //     }
  //   });
  //   subscription.on('data', function(log){
  //     console.log("data:=",log);
  //     web3.eth.clearSubscriptions();
  //   });

  //   // unsubscribes the subscription
  //   subscription.unsubscribe(function(error, success){
  //     if(success)
  //         console.log('Successfully unsubscribed!');
  //   });
  // }
  //for locklove
  getProprose(fromBlockNumber, toBlockNumber, callback) {
    console.log("getProprose");
    callback(null,"getProprose");
  }
  //for locklove
  getReplyProprose(fromBlockNumber, toBlockNumber, callback) {
    console.log("getReplyProprose");
    callback(null,"getReplyProprose");
  }
  //for locklove
  getMemory(fromBlockNumber, toBlockNumber, callback) {
    console.log("getMemory");
    callback(null,"getMemory");
  }
  //
  getBlockTimestamp (ret, callback) {
    start1 = Date.now();
    const blockTimestamps = {};
    const blockNumbersLog           = _.uniqBy(_.map(ret.logs, 'blockNumber'));
    const blockNumbersProprose      = _.uniqBy(_.map(ret.proprose, 'blockNumber'));
    const blockNumbersReplyProprose = _.uniqBy(_.map(ret.replyProprose, 'blockNumber'));
    const blockNumbersMemory        = _.uniqBy(_.map(ret.memory, 'blockNumber'));
    // console.log(blockNumbersProprose,'-',blockNumbersReplyProprose,'-',blockNumbersMemory);

    // /* Remove item uniqueness & undefined */
    const blockNumbers              = _.without(_.unionBy(blockNumbersLog,blockNumbersProprose,blockNumbersReplyProprose,blockNumbersMemory),undefined);

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
        return callback(_err);
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
      record.index        = item.index;
      record.fAddress     = item.fAddress;
      record.fPropose     = item.fPropose;
      record.fImg         = item.fImg;
      record.tAddress     = item.tAddress;
      // record.tPropose     = item.tPropose;
      // record.tImg         = item.tImg;
      let place           = "{"+"name:"+item.place+", "+ "longitude:"+item.long+", "+"latitude:"+item.lat+"}";
      record.place        = place;
      record.createtime   = timestamp;
      record.replytime    = timestamp;
    });
    callback(null,records);
  }

  //
  _processReplyProposeData (proposes, blockTimestamps, callback) {
    console.log("_processReplyProposeData: ",proposes);
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
      const record        = records[txid];
      record.index        = item.index;
      record.tPropose     = item.tPropose;
      record.tImg         = item.tImg;
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
      const record        = records[txid];
      record.index        = item.index;
      record.proposeID    = item.proposeID;
      record.fAddress     = item.fAddress;
      record.comment      = item.comment;
      record.tImg         = item.tImg;
      let place           = "{"+"name:"+item.place+", "+ "longitude:"+item.long+", "+"latitude:"+item.lat+"}";
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
      record.index        = log.blockNumber;
      record.blockNumber  = log.blockNumber;
      record.blockHash    = log.blockHash;
      record.blockTimestamp = timestamp;
      record.tx           = log.transactionHash;
      let place = "{"+"name:"+"HN"+", "+ "longitude:"+"item.long"+", "+"latitude:"+"item.lat"+"}";
      record.place        = place;
    });
    console.log("_processLogData done");
    callback(null,records);
  }

  //Test
  _saveToRds(users, memoris, proposes, callback) {
    async.waterfall([
      (next) => {
        async.eachLimit(_.values(users), PARALLEL_INSERT_LIMIT, (user, _next) => {
          // Remove property index form infor data before save.
          // delete user.index;
          var newObj = _.omit(user,["index"]);
          importMulti.hmset(rk("U",user.index),newObj);
          _next(null,null);
        }, next);
      },
      (next) => {
        async.eachLimit(_.values(memoris), PARALLEL_INSERT_LIMIT, (memory, _next) => {
          // Remove property index form infor data before save.
          var newObj = _.omit(memory,["index"]);
          importMulti.hmset(rk("M",memory.index),newObj);
          _next(null,null);
        }, next);
      },
      (next) => {
        async.eachLimit(_.values(proposes), PARALLEL_INSERT_LIMIT, (propose, _next) => {
          // Remove property index form infor data before save.
          var newObj = _.omit(propose,["index"]);
          importMulti.hmset(rk("P",propose.index),newObj);
          _next(null,null);
        }, next);
      },
      (next) => {
        importMulti.exec(function(err,results){
          if (err) { next(err); } else {
            //console.log(results); 
            next(null,null);
           }
        });
      }
    ], (err, ret) => {
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
}

function rk(...args) {
  return Array.prototype.slice.call(args).join(':');
}
module.exports = SyncChainToRedis;
