package com.caeycae;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.converter.ToAvroConverterBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;


/**
 * Creado por mysery el 25/09/18.
 */
public class KafkaJsonMessageAvroConverter extends ToAvroConverterBase<String, byte[]> {
  public static final String SCHEMA =
      "{\"namespace\":\"com.sem.tracker.avro\",\"type\":\"record\",\"name\":\"KafkaMessageAvro\",\"fields\":[{\"name\":\"referrer\",\"type\":[\"string\",\"null\"]},{\"name\":\"redirecturl\",\"type\":[\"string\",\"null\"]},{\"name\":\"url\",\"type\":[\"string\",\"null\"]},{\"name\":\"trackeame_user_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"click_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"ip\",\"type\":[\"string\",\"null\"]},{\"name\":\"date_time\",\"type\":[\"string\",\"null\"]},{\"name\":\"type\",\"type\":[\"string\",\"null\"]},{\"name\":\"client_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"source\",\"type\":[\"string\",\"null\"]},{\"name\":\"traffic\",\"type\":[\"string\",\"null\"]},{\"name\":\"other_fields_json\",\"type\":[\"string\",\"null\"]},{\"name\":\"event_date\",\"type\":[\"string\",\"null\"]}]}";
  private static final String AVRO_REFERRER_KEY = "referrer";
  private static final String AVRO_REDIRECTURL_KEY = "redirecturl";
  private static final String AVRO_URL_KEY = "url";
  private static final String AVRO_USER_ID_KEY = "trackeame_user_id";
  private static final String AVRO_CLICK_ID_KEY = "click_id";
  private static final String AVRO_IP_KEY = "ip";
  private static final String AVRO_DATE_TIME_KEY = "date_time";
  private static final String AVRO_TYPE_KEY = "type";
  private static final String AVRO_CLIENT_ID_KEY = "client_id";
  private static final String AVRO_SOURCE_KEY = "source";
  private static final String AVRO_TRAFFIC_KEY = "traffic";
  private static final String AVRO_OTHERS_FIELDS_KEY = "other_fields_json";
  private static final String AVRO_EVENT_DATE_KEY = "event_date";
  private static final String JSON_REFERRER_PATH = "referrer";
  private static final String JSON_REDIRECTURL_PATH = "invoking_url";
  private static final String JSON_URL_PATH = "url";
  private static final String JSON_USER_ID_PATH = "user_id";
  private static final String JSON_CLICK_ID_PATH = "click_id";
  private static final String JSON_IP_PATH = "ip";
  private static final String JSON_DATE_PATH = "date";
  private static final String JSON_TYPE_PATH = "type";
  private static final String JSON_CLIENT_ID_PATH = "client_id";
  private static final String JSON_SOURCE_PATH = "source";
  private static final String JSON_TRAFFIC_PATH = "traffic";
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  private static final Gson GSON = new Gson();
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonMessageAvroConverter.class);

  @Override
  public Schema convertSchema(String schema, WorkUnitState workUnit) {
    LOGGER.debug("schema to {} : {}", schema, SCHEMA);
    return new Schema.Parser().parse(SCHEMA);
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, byte[] payload, WorkUnitState workUnit) {
    String jsonString;
    JsonObject jsonObject;
    jsonString = new String(payload, CHARSET);
    LOGGER.trace("RecordToConvert: {}", jsonString);

    try {
      jsonObject = GSON.fromJson(jsonString, JsonObject.class);
      if (jsonObject == null) {
        throw new RuntimeException("null json object");
      }
    } catch (JsonSyntaxException e) {
      LOGGER.error("Caught exception while parsing JSON string '" + jsonString + "'.");
      throw new RuntimeException(e);
    }

    //para monorail tenemos un sub elemento comunmente event.
    final String prefix = workUnit.getJobState().getProp("converter.despegar.avro.prefix");
    if (StringUtils.isNotBlank(prefix)) {
      jsonObject = (jsonObject.get(prefix) != null && !jsonObject.get(prefix).isJsonNull()) ? jsonObject.get(prefix)
          .getAsJsonObject() : jsonObject;
      jsonString = jsonObject.toString();
      LOGGER.trace("Message with prefix {}: {}", prefix, jsonString);
    }
    GenericRecord record = new GenericData.Record(outputSchema);
    record.put(AVRO_REFERRER_KEY, getSecureStringField(JSON_REFERRER_PATH, jsonObject));
    record.put(AVRO_REDIRECTURL_KEY, getSecureStringField(JSON_REDIRECTURL_PATH, jsonObject));
    record.put(AVRO_URL_KEY, getSecureStringField(JSON_URL_PATH, jsonObject));
    record.put(AVRO_USER_ID_KEY, getSecureStringField(JSON_USER_ID_PATH, jsonObject));
    record.put(AVRO_CLICK_ID_KEY, getSecureStringField(JSON_CLICK_ID_PATH, jsonObject));
    record.put(AVRO_IP_KEY, getSecureStringField(JSON_IP_PATH, jsonObject));
    record.put(AVRO_DATE_TIME_KEY, getSecureStringField(JSON_DATE_PATH, jsonObject));
    record.put(AVRO_TYPE_KEY, getSecureStringField(JSON_TYPE_PATH, jsonObject));
    record.put(AVRO_CLIENT_ID_KEY, getSecureStringField(JSON_CLIENT_ID_PATH, jsonObject));
    record.put(AVRO_SOURCE_KEY, getSecureStringField(JSON_SOURCE_PATH, jsonObject));
    record.put(AVRO_TRAFFIC_KEY, getSecureStringField(JSON_TRAFFIC_PATH, jsonObject));
    record.put(AVRO_EVENT_DATE_KEY, convertToDate(getSecureStringField(JSON_DATE_PATH, jsonObject)));
    record.put(AVRO_OTHERS_FIELDS_KEY, jsonString);

    LOGGER.trace("RecordToConvert from json to avro");

    return new SingleRecordIterable<>(record);
  }

  /**
   * for TO_DATE hive before 2.1.0 need a date like 'yyyy-MM-dd'.
   *
   * @param date_time date in format: yyyy-MM-dd HH:mm:ss:SSS
   * @return date in format: yyyy-MM-dd
   */
  private String convertToDate(String date_time) {
    final String date = date_time.substring(0, 10);
    LOGGER.trace("convertDate: {} to {}", date_time, date);
    return date;
  }

  private String getSecureStringField(String key, JsonObject jsonObject) {
    return (jsonObject.get(key) != null && !jsonObject.get(key).isJsonNull()) ? jsonObject.get(key).getAsString() : "";
  }
}