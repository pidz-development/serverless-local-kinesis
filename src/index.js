import path from 'path';
import { Kinesis } from 'aws-sdk';
import Kinesalite from 'kinesalite';
import _ from 'lodash';

const webpackFolder = '.webpack';

export class ServerlessLocalKinesis {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.serverlessLog = serverless.cli.log.bind(serverless.cli);
    this.kinesis = new Kinesis({
      endpoint: 'http://localhost:4567',
      region: 'us-east-1',
    });

    this.hooks = {
      'before:offline:start': this.run.bind(this),
    };
  }

  run = async () => {
    const port = this.serverless.service.custom.kinesis.port || 4567;
    const streamName = this.serverless.service.custom.kinesis.streamName || 'stream';
    const shards = this.serverless.service.custom.kinesis.shards || 1;

    if (streamName === '') throw new Error('No stream name is given');

    try {
      await this.createKinesis(port);

      await this.createStream(streamName, shards);

      await this.watchEvents(streamName);
    } catch (e) {
      this.serverlessLog(e);
    }
  };

  createStream = (streamName, shards) => {
    return new Promise(async (resolve, reject) => {
      try {
        await this.kinesis.createStream({ StreamName: streamName, ShardCount: shards }).promise();

        setTimeout(async () => {
          const stream = await this.kinesis.describeStream({ StreamName: streamName }).promise();

          this.serverlessLog(`🎊 Stream '${stream.StreamDescription.StreamName}' created with ${stream.StreamDescription.Shards.length} shard(s)`);

          resolve();
        }, 1000);
      } catch (e) {
        reject(e);
      }
    });
  };

  createKinesis = (port) => {
    const server = new Kinesalite();

    return new Promise((resolve, reject) => {
      server.listen(port, (error) => {
        if (error) {
          reject(error);
        }

        this.serverlessLog(`🚀 Local kinesis is running at ${port}`);

        resolve();
      });
    });
  };

  watchEvents = async (streamName) => {
    const stream = await this.kinesis.describeStream({ StreamName: streamName }).promise();

    const { ShardId } = stream.StreamDescription.Shards[0];

    const params = { StreamName: streamName, ShardId, ShardIteratorType: 'LATEST' };

    const shardIterator = await this.kinesis.getShardIterator(params).promise();

    let functions = [];

    for (const name of _.keys(this.serverless.service.functions)) {
      const serverlessFunction = this.serverless.service.functions[name];

      const streamEvent = _.filter(serverlessFunction.events || [], e => 'stream' in e);

      if (Array.isArray(streamEvent) && streamEvent.length > 0) {
        functions.push(serverlessFunction.handler);
      }
    }

    // added two spaces after the emoji so the log looks fine
    this.serverlessLog('⏰  Polling for events');

    this.pollKinesis(functions)(shardIterator.ShardIterator);
  };

  pollKinesis = functions => (firstShardIterator) => {
    const mapKinesisRecord = record => ({
      data: record.Data.toString('base64'),
      sequenceNumber: record.SequenceNumber,
      approximateArrivalTimestamp: record.ApproximateArrivalTimestamp,
      partitionKey: record.PartitionKey,
    });

    const reduceRecord = functions => (promise, kinesisRecord) => promise.then(() => {
      const singleRecordEvent = { Records: [{ kinesis: mapKinesisRecord(kinesisRecord) }] };

      functions.forEach(async (handler) => {
        handler = handler.split('.');

        const moduleFileName = `${handler[0]}.js`;
        const handlerFilePath = path.join(this.serverless.config.servicePath, webpackFolder, 'service', moduleFileName);
        const module = require(handlerFilePath);
        const functionObjectPath = handler.slice(1);

        let mod = module;

        for (let p of functionObjectPath) {
          mod = mod[p];
        }

        this.serverlessLog(`🤗 Invoking lambda '${handler[0]}.${handler[1]}'`);

        return mod(singleRecordEvent);
      });
    });

    const fetchAndProcessRecords = async (shardIterator) => {
      const records = await this.kinesis.getRecords({ ShardIterator: shardIterator }).promise();

      await records.Records.reduce(reduceRecord(functions), Promise.resolve());

      setTimeout(async () => {
        await fetchAndProcessRecords(records.NextShardIterator);
      }, 1000);
    };

    return fetchAndProcessRecords(firstShardIterator);
  };
}

module.exports = ServerlessLocalKinesis;
