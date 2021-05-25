package com.webtrekk.datastreams.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CSVLogger {

    private static final Logger LOGGER_CSV_META = LoggerFactory.getLogger("csv-meta");
    private static final Logger LOGGER_CSV_RECORDS = LoggerFactory.getLogger("csv-records");

    private boolean enableCsvLos = false;

    public CSVLogger(boolean enableCsvLogs) {
        this.enableCsvLos = enableCsvLogs;
        if (enableCsvLos) {
            LOGGER_CSV_RECORDS.debug("consumeTimestamp,kafkaPartition,kafkaOffset,keySize,valueSize,recordTimestamp,record");
            LOGGER_CSV_META.debug("timestamp,p0Records,p0Size,p0Lag,p1Records,p1Size,p1Lag,p2Records,p2Size,p2Lag,pollDuration,commitDuration");
        }
    }

    public void printMeta(Map<Integer, Integer> numberOfRecords,
                          Map<Integer, Integer> sizeOfRecords,
                          Map<Integer, Long> maxLags,
                          long pollDurationMillis,
                          String consumeTs,
                          long commitDurationMillis) {
        if (enableCsvLos) {
            LOGGER_CSV_META.debug(
                    consumeTs + "," +
                    numberOfRecords.getOrDefault(0, 0) + "," +
                    sizeOfRecords.getOrDefault(0, 0) + "," +
                    maxLags.getOrDefault(0, 0L) + "," +
                    numberOfRecords.getOrDefault(1, 0) + "," +
                    sizeOfRecords.getOrDefault(1, 0) + "," +
                    maxLags.getOrDefault(1, 0L) + "," +
                    numberOfRecords.getOrDefault(2, 0) + "," +
                    sizeOfRecords.getOrDefault(2, 0) + "," +
                    maxLags.getOrDefault(2, 0L) + "," +
                    pollDurationMillis + "," +
                    commitDurationMillis
            );
        }
    }

    public void logRecordInformation(String consumeTs,
                                     ConsumerRecord<byte[], String> record,
                                     int keySize,
                                     int valueSize,
                                     int partition,
                                     String recordTs) {
        if (this.enableCsvLos) {
            LOGGER_CSV_RECORDS.debug(
                    consumeTs + "," +
                    partition + "," +
                    record.offset() + "," +
                    keySize + "," +
                    valueSize + "," +
                    recordTs + "," +
                    record.value()
            );
        }
    }
}
