# Test & Analytics Kafka Consumer for Mapp (Webtrekk) Data Streams

## About

This project / application is about to gain insights about Mapp (Webtrekk) Data Streams. The simple Kafka consumer can
be configured such that one can consume Tracking Events. The application prints / logs useful metrics to test and
analyze the behavior of Mapp (Webtrekk) Data Streams.

## Configuration (before you run it)

The application uses an `application.properties` file to configure the standalone java application. The configuration
file needs to be provided when running the app on console.

Please find a template of the properties file in the `./dist` folder.

To complete the configuration add the following – sensitive – information provided by Mapp (Webtrekk):

```
consumer-group=***TBD***
topic=***TBD***
username=***TBD***
password=***TBD***
```

Additionally, the following advanced settings can be adjusted to meet the individual needs:

```
poll-timeout=1000
max-partition-fetch_bytes=1048576
max-poll-records=1000 (Kafka default: 500)
```

## Run

A precompiled, runnable – fat – jar is provided in the `./dist` folder in the latest version.

To run the application simply navigate into the `./dist` folder and run:

```
java -jar webtrekk-data-streams-consumer-test.jar ./application.properties
```

Don't forget to update the `application.propertes` beforehand as explained above.

## Logging for test & analytics

The app uses simple console logging to log metrics after each consumption from the data stream. Additionally, logging of
the Kafka Consumer can be turned on. Also logging of further metrics to file is possible.

### Kafka Client Logs

Setting `enable-kafka-client-debug-logs` to `true` in the `application.propertis` file will enable additional debug logs
of the Kafka consumer on console.

### TSV (tab separated values) logging

Setting `enable-tsv-logs` to `true` in the `application.propertis` file will enable additional logging to 2 TSV files:

- **meta.tsv**: Metrics about consumption of the data stream.
- **records:tsv**: Information about each consumed record

Note: The files are overwritten on every start of the application.

## Build it

The project uses the maven assembly plugin to build a runnable – fat – jar containing all dependencies.

Use `mvn package` on project root to build the jar. The jar will be located in the `./target/` folder of the project.
