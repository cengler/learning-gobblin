package com.caeycae;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Creado por mysery el 25/09/18.
 */
public class KafkaJsonMessageTimePartitioner extends TimeBasedWriterPartitioner<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonMessageTimePartitioner.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";
    private static final String UTC = "UTC";
    private static final String PARTITION_FIELD = "date";
    private Gson gson = new Gson();


    public KafkaJsonMessageTimePartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
    }

    @Override
    public long getRecordTimestamp(byte[] payload) {
        String payloadString;
        JsonObject jsonObject;
        try {
            payloadString = new String(payload, "UTF-8");
            LOGGER.debug("payloadString {}", payloadString);

        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Unable to load UTF-8 encoding, falling back to system default", e);
            payloadString = new String(payload);
        }

        try {
            jsonObject = gson.fromJson(payloadString, JsonObject.class);
        } catch (RuntimeException e) {
            LOGGER.error("Caught exception while parsing JSON string '{}'.", payloadString);
            throw new RuntimeException(e);
        }

        long ret = System.currentTimeMillis();
        if (jsonObject != null && jsonObject.has(PARTITION_FIELD)) {
            String date = jsonObject.get(PARTITION_FIELD).getAsString();
            ret = LocalDateTime.parse(date, DateTimeFormatter.ofPattern(DATE_FORMAT)).atZone(ZoneId.of(UTC)).toInstant().toEpochMilli();
        }

        return ret;
    }
}