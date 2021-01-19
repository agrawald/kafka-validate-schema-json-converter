/*
 * Copyright © 2019 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.com.mayi;

import static org.apache.kafka.common.utils.Utils.mkSet;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.report.LogLevel;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StringConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Converter that uses JSON to store schemas and objects. By default this
 * converter will serialize Connect keys, values, and headers with schemas, although this can be
 * disabled with {@link ValidateSchemaJsonConverterConfig#SCHEMAS_ENABLE_CONFIG schemas.enable} configuration
 * option.
 *
 * <p>This implementation currently does nothing with the topic names or header names.
 */
public class ValidateSchemaJsonConverter implements Converter, HeaderConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidateSchemaJsonConverter.class);

  private static final Map<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS =
      new EnumMap<>(Schema.Type.class);

  static {
    TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, (schema, value) -> value.booleanValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, (schema, value) -> (byte) value.intValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, (schema, value) -> (short) value.intValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, (schema, value) -> value.intValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, (schema, value) -> value.longValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, (schema, value) -> value.floatValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, (schema, value) -> value.doubleValue());
    TO_CONNECT_CONVERTERS.put(
        Schema.Type.BYTES,
        (schema, value) -> {
          try {
            return value.binaryValue();
          } catch (IOException e) {
            throw new DataException("Invalid bytes field", e);
          }
        });
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, (schema, value) -> value.textValue());
    TO_CONNECT_CONVERTERS.put(
        Schema.Type.ARRAY,
        (schema, value) -> {
          Schema elemSchema = schema == null ? null : schema.valueSchema();
          ArrayList<Object> result = new ArrayList<>();
          for (JsonNode elem : value) {
            result.add(convertToConnect(null, elemSchema, elem));
          }
          return result;
        });
    TO_CONNECT_CONVERTERS.put(
        Schema.Type.MAP,
        (schema, value) -> {
          Schema keySchema = schema == null ? null : schema.keySchema();
          Schema valueSchema = schema == null ? null : schema.valueSchema();

          // If the map uses strings for keys, it should be encoded in the natural JSON format. If
          // it uses other
          // primitive types or a complex type as a key, it will be encoded as a list of pairs. If
          // we don't have a
          // schema, we default to encoding in a Map.
          Map<Object, Object> result = new HashMap<>();
          if (schema == null || keySchema.type() == Schema.Type.STRING) {
            if (!value.isObject())
              throw new DataException(
                  "Maps with string fields should be encoded as JSON objects, but found "
                      + value.getNodeType());
            Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
            while (fieldIt.hasNext()) {
              Map.Entry<String, JsonNode> entry = fieldIt.next();
              result.put(
                  entry.getKey(), convertToConnect(entry.getKey(), valueSchema, entry.getValue()));
            }
          } else {
            if (!value.isArray())
              throw new DataException(
                  "Maps with non-string fields should be encoded as JSON array of tuples, but found "
                      + value.getNodeType());
            for (JsonNode entry : value) {
              if (!entry.isArray())
                throw new DataException(
                    "Found invalid map entry instead of array tuple: " + entry.getNodeType());
              if (entry.size() != 2)
                throw new DataException(
                    "Found invalid map entry, expected length 2 but found :" + entry.size());
              result.put(
                  convertToConnect(null, keySchema, entry.get(0)),
                  convertToConnect(null, valueSchema, entry.get(1)));
            }
          }
          return result;
        });
    TO_CONNECT_CONVERTERS.put(
        Schema.Type.STRUCT,
        (schema, value) -> {
          if (!value.isObject())
            throw new DataException(
                "Structs should be encoded as JSON objects, but found " + value.getNodeType());

          // We only have ISchema here but need Schema, so we need to materialize the actual schema.
          // Using ISchema
          // avoids having to materialize the schema for non-Struct types but it cannot be avoided
          // for Structs since
          // they require a schema to be provided at construction. However, the schema is only a
          // SchemaBuilder during
          // translation of schemas to JSON; during the more common translation of data to JSON, the
          // call to schema.schema()
          // just returns the schema Object and has no overhead.
          Struct result = new Struct(schema.schema());
          for (Field field : schema.fields())
            result.put(
                field, convertToConnect(field.name(), field.schema(), value.get(field.name())));

          return result;
        });
  }

  // Convert values in Kafka Connect form into/from their logical types. These logical converters
  // are discovered by logical type
  // names specified in the field
  private static final HashMap<String, LogicalTypeConverter> LOGICAL_CONVERTERS = new HashMap<>();

  private static final JsonNodeFactory JSON_NODE_FACTORY =
      JsonNodeFactory.withExactBigDecimals(true);

  static {
    LOGICAL_CONVERTERS.put(
        Decimal.LOGICAL_NAME,
        new LogicalTypeConverter() {
          @Override
          public JsonNode toJson(
              final Schema schema, final Object value, final ValidateSchemaJsonConverterConfig config) {
            if (!(value instanceof BigDecimal))
              throw new DataException(
                  "Invalid type for Decimal, expected BigDecimal but was " + value.getClass());

            final BigDecimal decimal = (BigDecimal) value;
            switch (config.decimalFormat()) {
              case NUMERIC:
                return JSON_NODE_FACTORY.numberNode(decimal);
              case BASE64:
                return JSON_NODE_FACTORY.binaryNode(Decimal.fromLogical(schema, decimal));
              default:
                throw new DataException(
                    "Unexpected "
                        + ValidateSchemaJsonConverterConfig.DECIMAL_FORMAT_CONFIG
                        + ": "
                        + config.decimalFormat());
            }
          }

          @Override
          public Object toConnect(final Schema schema, final JsonNode value) {
            if (value.isNumber()) return value.decimalValue();
            if (value.isBinary() || value.isTextual()) {
              try {
                return Decimal.toLogical(schema, value.binaryValue());
              } catch (Exception e) {
                throw new DataException("Invalid bytes for Decimal field", e);
              }
            }

            throw new DataException(
                "Invalid type for Decimal, underlying representation should be numeric or bytes but was "
                    + value.getNodeType());
          }
        });

    LOGICAL_CONVERTERS.put(
        Date.LOGICAL_NAME,
        new LogicalTypeConverter() {
          @Override
          public JsonNode toJson(
              final Schema schema, final Object value, final ValidateSchemaJsonConverterConfig config) {
            if (!(value instanceof java.util.Date))
              throw new DataException(
                  "Invalid type for Date, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.numberNode(Date.fromLogical(schema, (java.util.Date) value));
          }

          @Override
          public Object toConnect(final Schema schema, final JsonNode value) {
            if (!(value.isInt()))
              throw new DataException(
                  "Invalid type for Date, underlying representation should be integer but was "
                      + value.getNodeType());
            return Date.toLogical(schema, value.intValue());
          }
        });

    LOGICAL_CONVERTERS.put(
        Time.LOGICAL_NAME,
        new LogicalTypeConverter() {
          @Override
          public JsonNode toJson(
              final Schema schema, final Object value, final ValidateSchemaJsonConverterConfig config) {
            if (!(value instanceof java.util.Date))
              throw new DataException(
                  "Invalid type for Time, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.numberNode(Time.fromLogical(schema, (java.util.Date) value));
          }

          @Override
          public Object toConnect(final Schema schema, final JsonNode value) {
            if (!(value.isInt()))
              throw new DataException(
                  "Invalid type for Time, underlying representation should be integer but was "
                      + value.getNodeType());
            return Time.toLogical(schema, value.intValue());
          }
        });

    LOGICAL_CONVERTERS.put(
        Timestamp.LOGICAL_NAME,
        new LogicalTypeConverter() {
          @Override
          public JsonNode toJson(
              final Schema schema, final Object value, final ValidateSchemaJsonConverterConfig config) {
            if (!(value instanceof java.util.Date))
              throw new DataException(
                  "Invalid type for Timestamp, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.numberNode(
                Timestamp.fromLogical(schema, (java.util.Date) value));
          }

          @Override
          public Object toConnect(final Schema schema, final JsonNode value) {
            if (!(value.isIntegralNumber()))
              throw new DataException(
                  "Invalid type for Timestamp, underlying representation should be integral but was "
                      + value.getNodeType());
            return Timestamp.toLogical(schema, value.longValue());
          }
        });
  }

  private ValidateSchemaJsonConverterConfig config;

  private final JsonSerializer serializer;
  private final JsonDeserializer deserializer;

  public ValidateSchemaJsonConverter() {
    serializer = new JsonSerializer(mkSet(), JSON_NODE_FACTORY);

    deserializer =
        new JsonDeserializer(
            mkSet(
                // this ensures that the JsonDeserializer maintains full precision on
                // floating point numbers that cannot fit into float64
                DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS),
            JSON_NODE_FACTORY);
  }

  @Override
  public ConfigDef config() {
    return ValidateSchemaJsonConverterConfig.configDef();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    config = new ValidateSchemaJsonConverterConfig(configs);

    serializer.configure(configs, config.type() == ConverterType.KEY);
    deserializer.configure(configs, config.type() == ConverterType.KEY);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Map<String, Object> conf = new HashMap<>(configs);
    conf.put(
        StringConverterConfig.TYPE_CONFIG,
        isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
    configure(conf);
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
    return fromConnectData(topic, schema, value);
  }

  @Override
  public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
    return toConnectData(topic, value);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (schema == null && value == null) {
      return null;
    }

    final JsonNode jsonValue = convertToJsonWithoutEnvelope(schema, value);
    try {
      return serializer.serialize(topic, jsonValue);
    } catch (SerializationException e) {
      throw new DataException(
          "Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    JsonNode jsonValue;

    // This handles a tombstone message
    if (value == null) {
      return SchemaAndValue.NULL;
    }

    try {
      jsonValue = deserializer.deserialize(topic, value);
    } catch (SerializationException e) {
      LOGGER.error(
          "==X Converting byte[] to Kafka Connect data failed due to serialization error: " + topic,
          e);
      throw new DataException(
          "Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
    }

    //validate the event against the json schema linked to the topic name, throws DataException if validation fails
    this.validateEventAgainstSchema(topic, jsonValue);

    final ObjectNode envelope = JSON_NODE_FACTORY.objectNode();
    envelope.set(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME, null);
    envelope.set(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME, jsonValue);
    jsonValue = envelope;

    return new SchemaAndValue(
        null, convertToConnect(null, null, jsonValue.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME)));
  }

  /**
   * Validates the event against the json schema linked to the topic
   * @param topic name of the topic for which json schema need to fetched
   * @param jsonValue json event to validate
   * @throws DataException when validation fails
   */
  private void validateEventAgainstSchema(final String topic, final JsonNode jsonValue) {
    // Check if the schemas are enabled
    if (config.schemasEnabled()) {
      // if yes then we have to identify the schema for this topic
      final ProcessingReport report = JsonSchemaValidator.validate(topic, jsonValue);
      // if validation failed, then throw DataException, notify the connector to park the event in DLQ if configured
      if (!report.isSuccess()) {
        final List<ProcessingMessage> messages =
                StreamSupport.stream(report.spliterator(), true)
                        .filter(
                                processingMessage ->
                                        processingMessage.getLogLevel() == LogLevel.ERROR
                                                || processingMessage.getLogLevel() == LogLevel.FATAL)
                        .collect(Collectors.toList());
        LOGGER.error(
                "==X Invalid event found! \nEvent: " + jsonValue.toString() + "\nErrors: " + messages);
        throw new DataException(
                "==X Invalid event found! \nEvent: " + jsonValue.toString() + "\nErrors: " + messages);
      }
    }
  }

  private JsonNode convertToJsonWithoutEnvelope(Schema schema, Object value) {
    return convertToJson(schema, value);
  }

  /**
   * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object, returning
   * both the schema and the converted object.
   */
  private JsonNode convertToJson(Schema schema, Object value) {
    if (value == null) {
      // Any schema is valid and we don't have a default, so treat this as an optional
      // schema
      if (schema == null) return null;
      if (schema.defaultValue() != null) return convertToJson(schema, schema.defaultValue());
      if (schema.isOptional()) return JSON_NODE_FACTORY.nullNode();
      throw new DataException(
          "Conversion error: null value for field that is required and has no default value");
    }

    if (schema != null && schema.name() != null) {
      LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.name());
      if (logicalConverter != null) return logicalConverter.toJson(schema, value, config);
    }

    try {
      final Schema.Type schemaType;
      if (schema == null) {
        schemaType = ConnectSchema.schemaType(value.getClass());
        if (schemaType == null)
          throw new DataException(
              "Java class " + value.getClass() + " does not have corresponding schema type.");
      } else {
        schemaType = schema.type();
      }
      switch (schemaType) {
        case INT8:
          return JSON_NODE_FACTORY.numberNode((Byte) value);
        case INT16:
          return JSON_NODE_FACTORY.numberNode((Short) value);
        case INT32:
          return JSON_NODE_FACTORY.numberNode((Integer) value);
        case INT64:
          return JSON_NODE_FACTORY.numberNode((Long) value);
        case FLOAT32:
          return JSON_NODE_FACTORY.numberNode((Float) value);
        case FLOAT64:
          return JSON_NODE_FACTORY.numberNode((Double) value);
        case BOOLEAN:
          return JSON_NODE_FACTORY.booleanNode((Boolean) value);
        case STRING:
          CharSequence charSeq = (CharSequence) value;
          return JSON_NODE_FACTORY.textNode(charSeq.toString());
        case BYTES:
          if (value instanceof byte[]) return JSON_NODE_FACTORY.binaryNode((byte[]) value);
          else if (value instanceof ByteBuffer)
            return JSON_NODE_FACTORY.binaryNode(((ByteBuffer) value).array());
          else throw new DataException("Invalid type for bytes type: " + value.getClass());
        case ARRAY:
          {
            Collection collection = (Collection) value;
            ArrayNode list = JSON_NODE_FACTORY.arrayNode();
            for (Object elem : collection) {
              Schema valueSchema = schema == null ? null : schema.valueSchema();
              JsonNode fieldValue = convertToJson(valueSchema, elem);
              list.add(fieldValue);
            }
            return list;
          }
        case MAP:
          {
            Map<?, ?> map = (Map<?, ?>) value;
            // If true, using string keys and JSON object; if false, using non-string keys and
            // Array-encoding
            boolean objectMode;
            if (schema == null) {
              objectMode = true;
              for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                  objectMode = false;
                  break;
                }
              }
            } else {
              objectMode = schema.keySchema().type() == Schema.Type.STRING;
            }
            ObjectNode obj = null;
            ArrayNode list = null;
            if (objectMode) obj = JSON_NODE_FACTORY.objectNode();
            else list = JSON_NODE_FACTORY.arrayNode();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
              Schema keySchema = schema == null ? null : schema.keySchema();
              Schema valueSchema = schema == null ? null : schema.valueSchema();
              JsonNode mapKey = convertToJson(keySchema, entry.getKey());
              JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

              if (objectMode) obj.set(mapKey.asText(), mapValue);
              else list.add(JSON_NODE_FACTORY.arrayNode().add(mapKey).add(mapValue));
            }
            return objectMode ? obj : list;
          }
        case STRUCT:
          {
            Struct struct = (Struct) value;
            if (!struct.schema().equals(schema)) throw new DataException("Mismatching schema.");
            ObjectNode obj = JSON_NODE_FACTORY.objectNode();
            for (Field field : schema.fields()) {
              obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
            }
            return obj;
          }
      }

      throw new DataException("Couldn't convert " + value + " to JSON.");
    } catch (ClassCastException e) {
      String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
      throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
    }
  }

  private static Object convertToConnect(final String jsonKey, Schema schema, JsonNode jsonValue) {
    final Schema.Type schemaType;
    if (schema != null) {
      schemaType = schema.type();
      if (jsonValue == null || jsonValue.isNull()) {
        if (schema.defaultValue() != null)
          return schema
              .defaultValue(); // any logical type conversions should already have been applied
        if (schema.isOptional()) return null;
        LOGGER.error(
            "==< Invalid null value for required "
                + schemaType
                + " Field: "
                + (jsonKey != null ? jsonKey : ""));
        throw new DataException("Invalid null value for required " + schemaType + " field");
      }
    } else {
      switch (jsonValue.getNodeType()) {
        case NULL:
        case MISSING:
          // Special case. With no schema
          return null;
        case BOOLEAN:
          schemaType = Schema.Type.BOOLEAN;
          break;
        case NUMBER:
          if (jsonValue.isIntegralNumber()) schemaType = Schema.Type.INT64;
          else schemaType = Schema.Type.FLOAT64;
          break;
        case ARRAY:
          schemaType = Schema.Type.ARRAY;
          break;
        case OBJECT:
          schemaType = Schema.Type.MAP;
          break;
        case STRING:
          schemaType = Schema.Type.STRING;
          break;

        case BINARY:
        case POJO:
        default:
          schemaType = null;
          break;
      }
    }

    final JsonToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
    if (typeConverter == null) {
      LOGGER.error(
          "==< Unknown schema type: " + schemaType + " Field: " + (jsonKey != null ? jsonKey : ""));
      throw new DataException("Unknown schema type: " + schemaType);
    }

    if (schema != null && schema.name() != null) {
      LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.name());
      if (logicalConverter != null) return logicalConverter.toConnect(schema, jsonValue);
    }

    return typeConverter.convert(schema, jsonValue);
  }

  private interface JsonToConnectTypeConverter {
    Object convert(Schema schema, JsonNode value);
  }

  private interface LogicalTypeConverter {
    JsonNode toJson(Schema schema, Object value, ValidateSchemaJsonConverterConfig config);

    Object toConnect(Schema schema, JsonNode value);
  }
}
