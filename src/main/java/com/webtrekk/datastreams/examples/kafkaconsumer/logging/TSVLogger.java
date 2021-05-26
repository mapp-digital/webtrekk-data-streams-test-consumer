package com.webtrekk.datastreams.examples.kafkaconsumer.logging;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSVLogger {

    private static final Logger LOGGER = LoggerFactory.getLogger(TSVLogger.class);

    private static final Logger LOGGER_TSV_META = LoggerFactory.getLogger("tsv-meta");
    private static final Logger LOGGER_TSV_RECORDS = LoggerFactory.getLogger("tsv-records");

    private final boolean enableTsvLos;

    public TSVLogger(boolean enableTsvLogs) {
        this.enableTsvLos = enableTsvLogs;
        if (enableTsvLos) {
            LOGGER.info("Enabling TSV logging");
            LOGGER_TSV_RECORDS.info("consumeTs\tpartition\toffset\tsizeKey\tsizeValue\trecordTimestamp\trecordValue");
            LOGGER_TSV_META.info("consumeTs\tnRecords\tsizeValue\tlagMaxMs\tpollDuration\tcommitDuration");
        }
    }

    public void printMeta(String consumeTs, Map<Integer, Integer> nRecordsPerPartition,
            Map<Integer, Integer> sizeValuePerPartition, Map<Integer, Long> lagMaxMsPerPartition, long pollDurationMs,
            long commitDurationMs) {
        if (enableTsvLos) {
            LOGGER_TSV_META.info(consumeTs + "\t" + nRecordsPerPartition + "\t" + sizeValuePerPartition + "\t"
                    + lagMaxMsPerPartition + "\t" + pollDurationMs + "\t" + commitDurationMs);
        }
    }

    public void logRecordInformation(String consumeTs, String recordTs, ConsumerRecord<byte[], String> record,
            int keySize, int valueSize) {
        if (this.enableTsvLos) {
            LOGGER_TSV_RECORDS.info(consumeTs + "\t" + record.partition() + "\t" + record.offset() + "\t" + keySize
                    + "\t" + valueSize + "\t" + recordTs + "\t" + record.value());
        }
    }
}
