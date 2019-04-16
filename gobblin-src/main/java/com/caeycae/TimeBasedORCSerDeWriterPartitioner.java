package com.caeycae;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Optional;

/**
 * Creado por mysery el 25/09/18.
 * A {@link TimeBasedWriterPartitioner} for {@link org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow}s.
 * <p>
 * The {@link org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow} that contains the timestamp can be specified using
 * {@link TimeBasedORCSerDeWriterPartitioner#WRITER_PARTITION_COLUMN_NAME}.
 * only accept simple value, e.g., "timestamp" or "event_date".
 * <p>
 * If multiple values are specified, they will be tried in order. In the above example, if a record contains a valid
 * "header.timestamp" field, its value will be used, otherwise "device.timestamp" will be used.
 * <p>
 * If a record contains none of the specified fields, or if no field is specified, the current timestamp will be used.
 */
public class TimeBasedORCSerDeWriterPartitioner extends TimeBasedWriterPartitioner<Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeBasedORCSerDeWriterPartitioner.class);
    private static final String WRITER_PARTITION_COLUMN_NAME = ConfigurationKeys.WRITER_PREFIX + ".partition.column";
    private static final String WRITER_PARTITION_COLUMN_INDEX = ConfigurationKeys.WRITER_PREFIX + ".partition.column.index";
    private static final String WRITER_PARTITION_COLUMN_PATTERN = ConfigurationKeys.WRITER_PREFIX + ".partition.column.pattern";

    private static final String UTC = "UTC";
    private static final String ORC_SERDE_INSPECTOR = "getInspector";
    private static final String ORC_SERDE_ROW = "realRow";


    private final Optional<String> partitionColumnName;
    private final Optional<Integer> partitionColumnIndex;
    private final Optional<String> partitionColumnPattern;
    private DateTimeFormatter partitionColumnDateTimeFormatter;

    public TimeBasedORCSerDeWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
        this.partitionColumnName = getPropertyAsString(WRITER_PARTITION_COLUMN_NAME, state, numBranches, branchId);
        this.partitionColumnIndex = getPropertyAsInteger(WRITER_PARTITION_COLUMN_INDEX, state, numBranches, branchId);
        this.partitionColumnPattern = getPropertyAsString(WRITER_PARTITION_COLUMN_PATTERN, state, numBranches, branchId);
        if (this.partitionColumnPattern.isPresent()) {
            LOGGER.debug("Partitioned field pattern: {}", partitionColumnPattern.get());
            this.partitionColumnDateTimeFormatter = DateTimeFormatter.ofPattern(partitionColumnPattern.get());
        }
    }

    private static Optional<Integer> getPropertyAsInteger(String property, State state, int numBranches, int branchId) {
        String propName = ForkOperatorUtils.getPropertyNameForBranch(property, numBranches, branchId);
        return state.contains(propName) ? Optional.of(state.getPropAsInt(propName)) : Optional.empty();
    }

    private static Optional<String> getPropertyAsString(String property, State state, int numBranches, int branchId) {
        String propName = ForkOperatorUtils.getPropertyNameForBranch(property, numBranches, branchId);
        return state.contains(propName) ? Optional.of(state.getProp(propName)) : Optional.empty();
    }

    /**
     * Check if the partition column value is present and is a Long object.
     * if partitionColumnPattern is present we format LocalDateTime as EpochMilli.
     * Otherwise, use current system time.
     */
    private long getRecordTimestamp(Optional<Object> writerPartitionColumnValue) {
        long ret = System.currentTimeMillis();
        if (!this.partitionColumnPattern.isPresent()) {
            if (writerPartitionColumnValue.orElse(null) instanceof Long)
                ret = (Long) writerPartitionColumnValue.get();
        } else {
            if (writerPartitionColumnValue.orElse(null) instanceof String)
                ret = LocalDateTime.parse((String) writerPartitionColumnValue.get(), partitionColumnDateTimeFormatter)
                        .atZone(ZoneId.of(UTC)).toInstant().toEpochMilli();
        }
        return ret;
    }

    @Override
    public long getRecordTimestamp(Object record) {
        return getRecordTimestamp(getWriterPartitionColumnValue(record));
    }

    /**
     * Retrieve the index of the partition column field specified by this.partitionColumnName. Is slowest performance.
     */
    private int extractTimestampIndex(Object orcRecord) {
        if (!this.partitionColumnName.isPresent()) {
            throw new IllegalArgumentException("writer.partition.column doesn't have value");
        }

        Class<?> clazz = orcRecord.getClass();
        try {
            Method method = clazz.getDeclaredMethod(ORC_SERDE_INSPECTOR);
            method.setAccessible(true);
            ObjectInspector inspector = (ObjectInspector) method.invoke(clazz.cast(orcRecord));
            return ((StandardStructObjectInspector) inspector).getStructFieldRef(partitionColumnName.get()).getFieldID();
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException("writer.partition.column: " + partitionColumnName.get() + " doesn't exist on row");
        }
    }

    /**
     * Retrieve the value of the partition column field specified by this.partitionColumnIndex.
     * If partitionColumnIndex not exist, this will seek it.
     */
    private Optional<Object> getWriterPartitionColumnValue(Object orcRecord) {
        if (!this.partitionColumnName.isPresent()) {
            return Optional.empty();
        }

        Class<?> clazz = orcRecord.getClass();
        ArrayList<Object> realRow;

        //only for check if column index is know.
        int timestampIndex = this.partitionColumnIndex.isPresent() ?
                this.partitionColumnIndex.get() : extractTimestampIndex(orcRecord);
        LOGGER.debug("Partitioned field index: {}", timestampIndex);
        try {
            Field field = clazz.getDeclaredField(ORC_SERDE_ROW);
            field.setAccessible(true);

            realRow = (ArrayList<Object>) field.get(orcRecord);
            if (realRow == null || realRow.size() <= timestampIndex) {
                return Optional.empty();
            }
            LOGGER.debug("partition by: {}", realRow.get(timestampIndex));
            return Optional.ofNullable(realRow.get(timestampIndex));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
