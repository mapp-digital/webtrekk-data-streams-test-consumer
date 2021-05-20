package com.webtrekk.playground.kafkaconsumer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

import static com.webtrekk.playground.kafkaconsumer.ConfigKeys.*;

public class KafkaConsumerExample {

    private static final DecimalFormat df = new DecimalFormat("#.#");

    private static final DateFormat dateFormat = initDateFormat();

    private static final String defaultConfigFile = "application";
    private static ResourceBundle config = null;

    public static void main(String[] args) throws IOException {
        config = loadResourceBundle(args);

        Duration pollTimeOut = Duration.ofMillis(Long.parseLong(config.getString(PollTimeout)));
        boolean enableKafkaClientLogs = Boolean.parseBoolean(config.getString(EnableKafkaClientLogs));
        boolean enableCsvLogs = Boolean.parseBoolean(config.getString(EnableCsvLogs));

        if (enableKafkaClientLogs) {
            BasicConfigurator.configure();
        }

        MyKafkaConsumerFactory kafkaConsumerFactory = new MyKafkaConsumerFactory();
        KafkaConsumer<byte[], String> consumer = kafkaConsumerFactory.getConsumer();

        List<String> headersRecords = Arrays.asList("consumeTimestamp", "kafkaPartition", "kafkaOffset", "keySize", "valueSize", "recordTimestamp", "record");
        CSVPrinter csvRecords = getCsvPrinter("record_sizes.csv", headersRecords);

        List<String> headersMeta = Arrays.asList("timestamp", "p0Records", "p0Size", "p0Lag", "p1Records", "p1Size", "p1Lag", "p2Records", "p2Size", "p2Lag", "pollDuration", "commitDuration");
        CSVPrinter csvMeta = getCsvPrinter("meta.csv", headersMeta);

        Map<Integer, Integer> numberOfRecords = new HashMap<>();
        Map<Integer, Integer> sizeOfRecords = new HashMap<>();
        Map<Integer, Long> maxLags = new HashMap<>();
        Map<Integer, Long> currentOffsets = new HashMap<>();
        Map<Integer, Integer> totalNumberOfRecords = new HashMap<>();
        Map<Integer, Integer> nConsumesPerPartition = new HashMap<>();

        long nSuccessfulPolls = 1;
        long nFailedPolls = 1;
        long sumRecords = 0;
        long sumPollDuration = 0;
        long sumNPartitions = 0;
        long sumSize = 0;

        long startMillis = System.currentTimeMillis();
        while (true) {
            long startPollMillis = System.currentTimeMillis();
            ConsumerRecords<byte[], String> records = consumer.poll(pollTimeOut);
            long pollDurationMillis = System.currentTimeMillis() - startPollMillis;

            String consumeTs = dateFormat.format(new Date(startPollMillis));

            int recordsPerPoll = 0;
            numberOfRecords.clear();
            sizeOfRecords.clear();
            maxLags.clear();
            currentOffsets.clear();

            Set<Integer> consumedPartitions = new HashSet<>();

            for (ConsumerRecord<byte[], String> record : records) {
                recordsPerPoll++;

                int keySize = record.key().length;
                int valueSize = record.value().length();
                int partition = record.partition();

                int count = numberOfRecords.getOrDefault(partition, 0);
                numberOfRecords.put(partition, count + 1);

                int totalCount = totalNumberOfRecords.getOrDefault(partition, 0);
                totalNumberOfRecords.put(partition, totalCount + 1);

                int size = sizeOfRecords.getOrDefault(partition, 0);
                sizeOfRecords.put(partition, size + valueSize);

                long recordLag = (System.currentTimeMillis() - record.timestamp()) / 1000L;
                long oldLag = maxLags.getOrDefault(partition, 0L);
                maxLags.put(partition, Math.max(oldLag, recordLag));

                currentOffsets.put(partition, record.offset());

                consumedPartitions.add(partition);

                String recordTs = dateFormat.format(new Date(record.timestamp()));
                if (enableCsvLogs) {
                    csvRecords.printRecord(consumeTs, partition, record.offset(), keySize, valueSize, recordTs, record.value());
                }
            }

            consumedPartitions.forEach(consumedPartition -> {
                int count = nConsumesPerPartition.getOrDefault(consumedPartition, 0);
                nConsumesPerPartition.put(consumedPartition, count + 1);
            });

            long beginCommitMillis = System.currentTimeMillis();
            if (recordsPerPoll > 0) {
                nSuccessfulPolls++;
                consumer.commitSync();
            } else  {
                nFailedPolls++;
            }
            long commitDurationMillis = System.currentTimeMillis() - beginCommitMillis;

            if (enableCsvLogs) {
                csvMeta.printRecord(consumeTs,
                        numberOfRecords.getOrDefault(0, 0),
                        sizeOfRecords.getOrDefault(0, 0),
                        maxLags.getOrDefault(0, 0L),
                        numberOfRecords.getOrDefault(1, 0),
                        sizeOfRecords.getOrDefault(1, 0),
                        maxLags.getOrDefault(1, 0L),
                        numberOfRecords.getOrDefault(2, 0),
                        sizeOfRecords.getOrDefault(2, 0),
                        maxLags.getOrDefault(2, 0L),
                        pollDurationMillis,
                        commitDurationMillis);
            }

            csvRecords.flush();
            csvMeta.flush();

            int sumValueSize = sizeOfRecords.values().stream().mapToInt(Integer::intValue).sum();

            sumPollDuration += pollDurationMillis;
            sumRecords += numberOfRecords.values().stream().mapToInt(Integer::intValue).sum();
            sumNPartitions += numberOfRecords.keySet().size();
            sumSize += sizeOfRecords.values().stream().mapToInt(Integer::intValue).sum();

            long totalDurationMillis = (System.currentTimeMillis() - startMillis);
            System.out.println(consumeTs +
                    " -> pollDuration: " + pollDurationMillis +
                    ", records: " + numberOfRecords +
                    ", totalRecords: " + totalNumberOfRecords +
                    ", recordsSum: " + recordsPerPoll +
                    ", size: " + sizeOfRecords +
                    ", sizeSum: " + sumValueSize +
                    ", lags: " + maxLags +
                    ", consumes: " + nConsumesPerPartition
            );
            System.out.println(
                    "   nSuccPolls: " + nSuccessfulPolls +
                    ", nFailedPolls: " + nFailedPolls +
                    ", avgPollDur: " + roundAndFormat(sumPollDuration, (nSuccessfulPolls+nFailedPolls)) +
                    ", avgNRecords: " + roundAndFormat(sumRecords, (nSuccessfulPolls+nFailedPolls)) +
                    ", avgRecords/s: " + roundAndFormat(sumRecords, (totalDurationMillis/1000)) +
                    ", avgNPartitions: " + roundAndFormat(sumNPartitions, (nSuccessfulPolls+nFailedPolls)) +
                    ", KByte/s: " + roundAndFormat((sumSize/1024), (totalDurationMillis/1000))
            );
        }

        // Close the consumer when necessary
        // consumer.close();
    }

    private static ResourceBundle loadResourceBundle(String[] args) throws IOException {
        if (args.length == 1) {
            FileInputStream fis = new FileInputStream(args[0]);
            return new PropertyResourceBundle(fis);
        } else {
            return ResourceBundle.getBundle(defaultConfigFile);
        }
    }

    private static DateFormat initDateFormat() {
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        f.setTimeZone(TimeZone.getTimeZone("UTC"));
        return f;
    }

    private static String roundAndFormat(long denominator, long nom) {
        return df.format(((double) denominator) / nom);
    }

    private static CSVPrinter getCsvPrinter(String fileNameMeta, List<String> headersMeta) throws IOException {
        FileWriter fWriterMeta = new FileWriter(fileNameMeta);
        CSVFormat csvFormatMeta = CSVFormat.DEFAULT.withHeader(headersMeta.stream().toArray(String[]::new));
        return new CSVPrinter(fWriterMeta,csvFormatMeta);
    }

    private static class MyKafkaConsumerFactory {

        public KafkaConsumer<byte[], String> getConsumer() {
            KafkaConsumer<byte[], String> consumer = new KafkaConsumer<>(getProperties(config));
            String topic = config.getString(Topic);
            System.out.println("Subscribing Kafka consumer to topic: " + topic);
            consumer.subscribe(Collections.singletonList(topic));
            return consumer;
        }

        private Properties getProperties(ResourceBundle config) {

            String keyDeserializer = ByteArrayDeserializer.class.getCanonicalName();
            String valueDeserializer = StringDeserializer.class.getCanonicalName();

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(Endpoints));

            props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getString(ClientId));
            props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString(GroupId));

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString(AutoOffsetResetPolicy));
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getString(EnableAutoCommit));
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getString(MaxPollRecords));
            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, config.getString(MaxPartitionFetchBytes));

            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);

            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getString(SecurityProtocol));
            props.put(SaslConfigs.SASL_MECHANISM, config.getString(SecuritySaslMechanism));
            props.put(SaslConfigs.SASL_JAAS_CONFIG, getJaasConfig(config));

            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.getString(SslTrustStoreLocation));
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.getString(SslTrustStorePassword));
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, config.getString(SslTrustStoreType));

            // props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024");
            return props;
        }

        private String getJaasConfig(ResourceBundle config) {
            String scramUser = config.getString(SecurityScramUsername);
            String scramPassword = config.getString(SecurityScramPassword);
            return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + scramUser + "\" password=\"" + scramPassword + "\";";
        }
    }
}