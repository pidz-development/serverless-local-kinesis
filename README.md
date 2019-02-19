# serverless-local-kinesis

Add the plugin to your project

```sh
yarn add -D serverless-local-kinesis
```

Add `serverless-local-kinesis` to your plugins

Configure the local kinesis stream

```yaml
custom:
  kinesis:
    port: 4567
    streamName: someEventName
    shards: 1
```

Polling mechanism is based on [Rabble Rousers's local-kinesis-lambda-runner](https://github.com/rabblerouser/local-kinesis-lambda-runner)

Thanks to [mhart](https://github.com/mhart) for creating [kinesalite](https://github.com/mhart/kinesalite)

Created by PIDZ Development for the open-source community :heart:
