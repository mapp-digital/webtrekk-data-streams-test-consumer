package com.webtrekk.datastreams.examples.kafkaconsumer.config;

public class ConfigKeys {

    public static final String PollTimeout = "poll-timeout";
    public static final String EnableKafkaDebugLogs = "enable-kafka-client-debug-logs";
    public static final String EnableTsvLogs = "enable-tsv-logs";

    public static final String Endpoints = "endpoints";

    public static final String ClientId = "client-id";
    public static final String GroupId = "consumer-group";

    public static final String EnableAutoCommit = "enable-auto-commit";
    public static final String MaxPollRecords = "max-poll-records";
    public static final String MaxPartitionFetchBytes = "max-partition-fetch_bytes";

    public static final String AutoOffsetResetPolicy = "auto-offset-reset-policy";

    public static final String SecurityProtocol = "security-protocol";
    public static final String SecuritySaslMechanism = "security-sasl-mechanism";
    public static final String SecurityScramUsername = "username";
    public static final String SecurityScramPassword = "password";

    public static final String SslTrustStoreLocation = "ssl-truststore-location";
    public static final String SslTrustStorePassword = "ssl-truststore-password";
    public static final String SslTrustStoreType = "ssl-truststore-type";

    public static final String Topic = "topic";
}
