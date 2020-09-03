import { Kinesis } from 'aws-sdk';
import { ShardIterator } from 'aws-sdk/clients/kinesis';
// @ts-ignore
import Kinesalite from 'kinesalite';
import { filter, keys } from 'lodash';
import * as path from 'path';

const webpackFolder = '.webpack';

class ServerlessLocalKinesis {
  private serverless: any;

  private readonly serverlessLog: any;

  private kinesis: Kinesis;

  private hooks: any;

  constructor(serverless: any, options: any) {
    this.serverless = serverless;
    this.serverlessLog = serverless.cli.log.bind(serverless.cli);
    this.kinesis = new Kinesis({
      endpoint: 'http://localhost:4567',
      region: 'us-east-1',
    });

    this.hooks = {
      'before:offline:start': this.run.bind(this),
      'before:offline:start:init': this.run.bind(this),
    };
  }

  public pollKinesis = (functions: string[]) => (firstShardIterator: ShardIterator) => {
    const mapKinesisRecord = (record: any) => ({
      approximateArrivalTimestamp: record.ApproximateArrivalTimestamp,
      data: record.Data.toString('base64'),
      partitionKey: record.PartitionKey,
      sequenceNumber: record.SequenceNumber,
    });

    const reduceRecord = (handlers: string[]) => (promise: any, kinesisRecord: any) => promise.then(() => {
      const singleRecordEvent = { Records: [{ kinesis: mapKinesisRecord(kinesisRecord) }] };

      handlers.forEach(async (handler: string) => {
        this.serverlessLog(`🤗 Invoking lambda '${handler}'`);

        const moduleFileName = `${handler.split('.')[0]}.js`;
        const handlerFilePath = path.join(this.serverless.config.servicePath, webpackFolder, 'service', moduleFileName);
        const module = require(handlerFilePath);
        const functionObjectPath = handler.split('.').slice(1);

        let mod = module;

        for (const p of functionObjectPath) {
          mod = mod[p];
        }

        return mod(singleRecordEvent);
      });
    });

    const fetchAndProcessRecords = async (shardIterator: ShardIterator) => {
      const records = await this.kinesis.getRecords({ ShardIterator: shardIterator }).promise();

      await records.Records.reduce(reduceRecord(functions), Promise.resolve());

      setTimeout(async () => {
        await fetchAndProcessRecords(records.NextShardIterator!);
      }, 1000);
    };

    return fetchAndProcessRecords(firstShardIterator);
  }

  private async run() {
    const port = this.serverless.service.custom.kinesis.port || 4567;
    const streamName = this.serverless.service.custom.kinesis.streamName || '';
    const shards = this.serverless.service.custom.kinesis.shards || 1;

    if (streamName === '') { throw new Error('No stream name is given'); }

    try {
      await this.createKinesis(port);

      await this.createStream(streamName, shards);

      await this.watchEvents(streamName);
    } catch (e) {
      this.serverlessLog(e);
    }
  }

  private createStream(streamName: string, shards: number): Promise<void> {
    return new Promise(async (resolve, reject) => {
      try {
        await this.kinesis.createStream({ StreamName: streamName, ShardCount: shards }).promise();

        setTimeout(async () => {
          const stream = await this.kinesis.describeStream({ StreamName: streamName }).promise();

          // tslint:disable-next-line:max-line-length
          this.serverlessLog(`🎊 Stream '${stream.StreamDescription.StreamName}' created with ${stream.StreamDescription.Shards.length} shard(s)`);

          resolve();
        }, 1000);
      } catch (e) {
        reject(e);
      }
    });
  }

  private createKinesis(port: number): Promise<void> {
    const server = new Kinesalite();

    return new Promise((resolve, reject) => {
      server.listen(port, (error: any) => {
        if (error) {
          reject(error);
        }

        this.serverlessLog(`🚀 Local kinesis is running at ${port}`);

        resolve();
      });
    });
  }

  private async watchEvents(streamName: string): Promise<void> {
    const stream = await this.kinesis.describeStream({ StreamName: streamName }).promise();

    const { ShardId } = stream.StreamDescription.Shards[0];

    const params = { StreamName: streamName, ShardId, ShardIteratorType: 'LATEST' };

    const shardIterator = await this.kinesis.getShardIterator(params).promise();

    const functions = [];

    for (const name of keys(this.serverless.service.functions)) {
      const serverlessFunction = this.serverless.service.functions[name];

      const streamEvent = filter(serverlessFunction.events || [], (e) => 'stream' in e);

      if (Array.isArray(streamEvent) && streamEvent.length > 0) {
        functions.push(serverlessFunction.handler);
      }
    }

    // added two spaces after the emoji so the log looks fine
    this.serverlessLog('⏰  Polling for events');

    this.pollKinesis(functions)(shardIterator.ShardIterator!);
  }
}

export = ServerlessLocalKinesis;
