package com.webtrekk.datastreams.examples.kafkaconsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.webtrekk.datastreams.examples.kafkaconsumer.config.ConfigKeys;
import com.webtrekk.datastreams.examples.kafkaconsumer.logging.TSVLogger;

public class KafkaConsumerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerTest.class);

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.#");
    private static final DateFormat DATE_FORMAT = initDateFormat();

    private static ResourceBundle config = null;

    public static void main(String[] args) throws IOException {
        config = loadResourceBundle(args);

        boolean enableTsvLogs = Boolean.parseBoolean(config.getString(ConfigKeys.EnableTsvLogs));
        TSVLogger TSV_LOGGER = new TSVLogger(enableTsvLogs);

        boolean enableKafkaClientLogs = Boolean.parseBoolean(config.getString(ConfigKeys.EnableKafkaDebugLogs));
        if (enableKafkaClientLogs) {
            LOGGER.info("Enabling Kafka client logs");
            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration conf = ctx.getConfiguration();
            conf.getLoggerConfig("org.apache.kafka").setLevel(Level.DEBUG);
            ctx.updateLoggers(conf);
        }

        MyKafkaConsumerFactory kafkaConsumerFactory = new MyKafkaConsumerFactory();
        KafkaConsumer<byte[], String> consumer = kafkaConsumerFactory.getConsumer();
        Duration pollTimeout = Duration.ofMillis(Long.parseLong(config.getString(ConfigKeys.PollTimeout)));
        boolean autoCommit = Boolean.parseBoolean(config.getString(ConfigKeys.EnableAutoCommit));

        Map<Integer, Integer> nRecordsPerPollPartition = new HashMap<>();
        Map<Integer, Integer> valueSizePerPollPartition = new HashMap<>();
        Map<Integer, Long> lagMaxMsPerPollPartition = new HashMap<>();

        Map<Integer, Long> totalNRecordsPerPartition = new HashMap<>();
        Map<Integer, Long> totalValueSizePerPartition = new HashMap<>();
        Map<Integer, Long> totalNConsumptionsPerPartition = new HashMap<>();

        long nPolls = 0;
        long nEmptyPolls = 0;
        long totalSumNRecords = 0;
        long totalPollDurationMs = 0;
        long totalSumNPartitions = 0;
        long totalSumSizeValue = 0;

        long startMs = System.currentTimeMillis();

        while (true) {
            long startPollMs = System.currentTimeMillis();
            ConsumerRecords<byte[], String> records = consumer.poll(pollTimeout);
            long pollDurationMs = System.currentTimeMillis() - startPollMs;

            nPolls++;
            String startPollTs = DATE_FORMAT.format(new Date(startPollMs));

            nRecordsPerPollPartition.clear();
            valueSizePerPollPartition.clear();
            lagMaxMsPerPollPartition.clear();

            for (ConsumerRecord<byte[], String> record : records) {
                int sizeKey = record.key().length;
                int sizeValue = record.value().length();
                int partition = record.partition();

                int nRecordsTmp = nRecordsPerPollPartition.getOrDefault(partition, 0);
                nRecordsPerPollPartition.put(partition, nRecordsTmp + 1);

                long totalNRecordsTmp = totalNRecordsPerPartition.getOrDefault(partition, 0L);
                totalNRecordsPerPartition.put(partition, totalNRecordsTmp + 1);

                int valueSizeTmp = valueSizePerPollPartition.getOrDefault(partition, 0);
                valueSizePerPollPartition.put(partition, valueSizeTmp + sizeValue);

                long totalValueSizeTmp = totalValueSizePerPartition.getOrDefault(partition, 0L);
                totalValueSizePerPartition.put(partition, totalValueSizeTmp + sizeValue);

                long lagRecordSec = (System.currentTimeMillis() - record.timestamp()) / 1000L;
                long lagMaxSec = Math.max(lagMaxMsPerPollPartition.getOrDefault(partition, 0L), lagRecordSec);
                lagMaxMsPerPollPartition.put(partition, lagMaxSec);

                String recordTs = DATE_FORMAT.format(new Date(record.timestamp()));
                TSV_LOGGER.logRecordInformation(startPollTs, recordTs, record, sizeKey, sizeValue);
            }

            Set<Integer> consumedPartitionsPerPoll = nRecordsPerPollPartition.keySet();
            consumedPartitionsPerPoll.forEach(consumedPartition -> {
                long nConsumptions = totalNConsumptionsPerPartition.getOrDefault(consumedPartition, 0L);
                totalNConsumptionsPerPartition.put(consumedPartition, nConsumptions + 1);
            });

            int sumNRecordsPerPoll = nRecordsPerPollPartition.values().stream().mapToInt(Integer::intValue).sum();
            int sumValueSizePerPoll = valueSizePerPollPartition.values().stream().mapToInt(Integer::intValue).sum();
            Set<Integer> assignedPartitions = consumer.assignment().stream().map(TopicPartition::partition)
                    .collect(Collectors.toSet());

            long startCommitMs = System.currentTimeMillis();
            if (sumNRecordsPerPoll > 0 && !autoCommit) {
                consumer.commitSync();
            } else {
                nEmptyPolls++;
            }
            long commitDurationMillis = System.currentTimeMillis() - startCommitMs;

            TSV_LOGGER.LogMetaInformation(startPollTs, nRecordsPerPollPartition, valueSizePerPollPartition,
                    lagMaxMsPerPollPartition, pollDurationMs, commitDurationMillis);

            totalPollDurationMs += pollDurationMs;
            totalSumNRecords += sumNRecordsPerPoll;
            totalSumNPartitions += nRecordsPerPollPartition.keySet().size();
            totalSumSizeValue += valueSizePerPollPartition.values().stream().mapToInt(Integer::intValue).sum();

            long totalDurationMs = (System.currentTimeMillis() - startMs);
            // statics for current poll
            LOGGER.info(startPollTs);
            LOGGER.info("    " + "pollDuration: " + pollDurationMs + ", nRecords: " + nRecordsPerPollPartition
                    + ", sumNRecords: " + sumNRecordsPerPoll + ", valueSize: " + valueSizePerPollPartition
                    + ", sumValueSize: " + sumValueSizePerPoll + ", maxLagsMs: " + lagMaxMsPerPollPartition
                    + ", assignedPartitions: " + assignedPartitions);
            // statistics over all polls
            LOGGER.info("    " + "nPolls: " + nPolls + ", nEmptyPolls: " + nEmptyPolls + ", totalNRecords: "
                    + totalNRecordsPerPartition + ", totalSumNRecords: " + totalSumNRecords + ", totalValueSize: "
                    + totalValueSizePerPartition + ", totalSumValueSize: " + totalSumSizeValue + ", consumptions: "
                    + totalNConsumptionsPerPartition);
            // average statistics over all polls
            LOGGER.info("    " + "avgPollDuration: " + roundAndFormat(totalPollDurationMs, nPolls)
                    + ", avgNRecords/poll: " + roundAndFormat(totalSumNRecords, nPolls) + ", avgNRecords/s: "
                    + roundAndFormat(totalSumNRecords, (totalDurationMs / 1000)) + ", avgNPartitions/poll: "
                    + roundAndFormat(totalSumNPartitions, (nPolls - nEmptyPolls)) + ", avgKByte/s: "
                    + roundAndFormat((totalSumSizeValue / 1024), (totalDurationMs / 1000)));
        }

        // Close the consumer when necessary
        // consumer.close();
    }

    private static ResourceBundle loadResourceBundle(String[] args) throws IOException {
        if (args.length == 1) {
            FileInputStream fis = new FileInputStream(args[0]);
            return new PropertyResourceBundle(fis);
        } else {
            LOGGER.error(
                    "No config provided. \n Usage: java -jar webtrekk-data-streams-consumer-test.jar \"./application.properties\"");
            System.exit(-1);
            return null;
        }
    }

    private static DateFormat initDateFormat() {
        DateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return simpleDateFormat;
    }

    private static String roundAndFormat(long denominator, long nom) {
        if (nom == 0) {
            return "n/a";
        } else {
            return DECIMAL_FORMAT.format(((double) denominator) / nom);
        }
    }

    private static class MyKafkaConsumerFactory {
        public KafkaConsumer<byte[], String> getConsumer() {
            KafkaConsumer<byte[], String> consumer = new KafkaConsumer<>(getProperties(config));
            String topic = config.getString(ConfigKeys.Topic);
            LOGGER.info("Subscribing Kafka consumer to topic: " + topic);
            consumer.subscribe(Collections.singletonList(topic));
            return consumer;
        }

        private Properties getProperties(ResourceBundle config) {
            String keyDeserializer = ByteArrayDeserializer.class.getCanonicalName();
            String valueDeserializer = StringDeserializer.class.getCanonicalName();

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(ConfigKeys.Endpoints));

            props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getString(ConfigKeys.ClientId));
            props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString(ConfigKeys.GroupId));

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString(ConfigKeys.AutoOffsetResetPolicy));
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getString(ConfigKeys.EnableAutoCommit));
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getString(ConfigKeys.MaxPollRecords));
            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                    config.getString(ConfigKeys.MaxPartitionFetchBytes));

            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);

            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getString(ConfigKeys.SecurityProtocol));
            props.put(SaslConfigs.SASL_MECHANISM, config.getString(ConfigKeys.SecuritySaslMechanism));
            props.put(SaslConfigs.SASL_JAAS_CONFIG, getJaasConfig(config));

            // props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            // config.getString(SslTrustStoreLocation));
            // props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            // config.getString(SslTrustStorePassword));
            // props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
            // config.getString(SslTrustStoreType));

            return props;
        }

        private String getJaasConfig(ResourceBundle config) {
            String scramUser = config.getString(ConfigKeys.SecurityScramUsername);
            String scramPassword = config.getString(ConfigKeys.SecurityScramPassword);
            return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + scramUser
                    + "\" password=\"" + scramPassword + "\";";
        }
    }
}