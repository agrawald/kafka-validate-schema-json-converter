package au.com.mayi;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JsonSchemaValidator will identify the schema to be used for validation by topic name and
 * then validate the payload against the json schema fetched from the file system.
 */
class JsonSchemaValidator {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonSchemaValidator.class);
  private static final Map<String, JsonSchema> SCHEMA_CACHE = new HashMap<>();
  private static final JsonSchemaFactory JSON_SCHEMA_FACTORY = JsonSchemaFactory.byDefault();

  /**
   * Load the schema for the topic, currently we support only the schema packaged with the library as resources
   * @param topic Name of the topic for which schema need to loaded
   * @return JsonSchema
   * @throws DataException if the schema not found, means event will be rejected and moved to DLQ if configured
   */
  private static JsonSchema loadSchema(final String topic) {
    final InputStream stream =
        JsonSchemaValidator.class
            .getClassLoader()
            .getResourceAsStream("schemas/" + topic + ".json");
    final JsonParser parser;
    LOGGER.info("==> Loading schema for topic: " + topic);
    try {
      parser = JSON_FACTORY.createParser(stream);
      final JsonNode jsonSchema = OBJECT_MAPPER.readTree(parser);
      final JsonSchema schema = JSON_SCHEMA_FACTORY.getJsonSchema(jsonSchema);
      SCHEMA_CACHE.put(topic, schema);
      return schema;
    } catch (IOException | ProcessingException e) {
      LOGGER.error("==< Unable to load schema for topic: " + topic, e);
      throw new DataException("Unable to load the schema for topic: " + topic, e);
    }
  }

  /**
   * Load the schema for the topic passed and then validates the json event against the schema loaded.
   * @param topic Name of the topic to fetch the schema
   * @param jsonValue The JSON event which needs to be validated
   * @return ProcessingReport
   */
  static ProcessingReport validate(final String topic, final JsonNode jsonValue) {
    // check if we already loaded the schema
    JsonSchema jsonSchema = SCHEMA_CACHE.get(topic);
    if (jsonSchema == null) {
      // load the schema in cache
      jsonSchema = loadSchema(topic);
    }
    try {
      // lets validate the event against the schema
      return jsonSchema.validate(jsonValue, true);
    } catch (ProcessingException pe) {
      LOGGER.error(
          "Unable to validate the JSON value against the schema. \nSchema: "
              + jsonSchema
              + " \nValue: "
              + jsonValue.toString(),
          pe);
      throw new DataException(
          "Unable to validate the JSON value against the schema. \nSchema: "
              + jsonSchema
              + " \nValue: "
              + jsonValue.toString(),
          pe);
    }
  }
}
