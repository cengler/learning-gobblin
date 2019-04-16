package com.caeycae;

import com.google.common.base.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.writer.partitioner.TimeBasedWriterPartitioner;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Creado por mysery el 25/09/18.
 */
public class AvroDateTimePartitioner extends TimeBasedWriterPartitioner<GenericRecord> {
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";
    private static final String UTC = "UTC";
    private static final String PARTITION_COLUMN = "date_time";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(DATE_FORMAT);

    public AvroDateTimePartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
    }

    @Override
    public long getRecordTimestamp(GenericRecord record) {
        Optional<Object> writerPartitionColumnValue = getWriterPartitionColumnValue(record);
        if (!writerPartitionColumnValue.isPresent())
            return System.currentTimeMillis();

        return LocalDateTime.parse((CharSequence) writerPartitionColumnValue.get(),
                FORMATTER).atZone(ZoneId.of(UTC)).toInstant().toEpochMilli();
    }

    /**
     * Retrieve the value of the partition column field specified by this.partitionColumns
     */
    private Optional<Object> getWriterPartitionColumnValue(GenericRecord record) {
        return AvroUtils.getFieldValue(record, PARTITION_COLUMN);
    }
}