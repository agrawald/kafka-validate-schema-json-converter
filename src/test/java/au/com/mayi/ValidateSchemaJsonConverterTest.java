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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ValidateSchemaJsonConverterTest {
  private static final String TOPIC = "topic";

  private final ObjectMapper objectMapper =
      new ObjectMapper()
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

  private final ValidateSchemaJsonConverter converter = new ValidateSchemaJsonConverter();

  @BeforeEach
  public void setUp() {
    converter.configure(Collections.emptyMap(), false);
  }

  // Schema types
  @Test
  public void nullToConnect() {
    // When schemas are enabled, trying to decode a tombstone should be an empty envelope
    // the behavior is the same as when the json is "{ "schema": null, "payload": null }"
    // to keep compatibility with the record
    SchemaAndValue converted = converter.toConnectData(TOPIC, null);
    assertEquals(SchemaAndValue.NULL, converted);
  }

  /**
   * When schemas are disabled, empty data should be decoded to an empty envelope. This test
   * verifies the case where `schemas.enable` configuration is set to false, and {@link
   * ValidateSchemaJsonConverter} converts empty bytes to {@link SchemaAndValue#NULL}.
   */
  @Test
  public void emptyBytesToConnect() {
    // This characterizes the messages with empty data when Json schemas is disabled
    Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
    converter.configure(props, true);
    SchemaAndValue converted = converter.toConnectData(TOPIC, "".getBytes());
    assertEquals(SchemaAndValue.NULL, converted);
  }

  /** When schemas are disabled, fields are mapped to Connect maps. */
  @Test
  public void schemalessWithEmptyFieldValueToConnect() {
    // This characterizes the messages with empty data when Json schemas is disabled
    Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
    converter.configure(props, true);
    String input = "{ \"a\": \"\", \"b\": null}";
    SchemaAndValue converted = converter.toConnectData(TOPIC, input.getBytes());
    Map<String, String> expected = new HashMap<>();
    expected.put("a", "");
    expected.put("b", null);
    assertEquals(new SchemaAndValue(null, expected), converted);
  }

  // Schema metadata

  @Test
  public void structSchemaIdentical() {
    Schema schema =
        SchemaBuilder.struct()
            .field("field1", Schema.BOOLEAN_SCHEMA)
            .field("field2", Schema.STRING_SCHEMA)
            .field("field3", Schema.STRING_SCHEMA)
            .field("field4", Schema.BOOLEAN_SCHEMA)
            .build();
    Schema inputSchema =
        SchemaBuilder.struct()
            .field("field1", Schema.BOOLEAN_SCHEMA)
            .field("field2", Schema.STRING_SCHEMA)
            .field("field3", Schema.STRING_SCHEMA)
            .field("field4", Schema.BOOLEAN_SCHEMA)
            .build();
    Struct input =
        new Struct(inputSchema)
            .put("field1", true)
            .put("field2", "string2")
            .put("field3", "string3")
            .put("field4", false);
    assertStructSchemaEqual(schema, input);
  }

  @Test
  public void decimalToJsonWithoutSchema() {
    assertThrows(
        DataException.class,
        () -> converter.fromConnectData(TOPIC, null, new BigDecimal(new BigInteger("156"), 2)));
  }

  @Test
  public void nullSchemaAndNullValueToJson() {
    // This characterizes the production of tombstone messages when Json schemas is enabled
    Map<String, Boolean> props = Collections.singletonMap("schemas.enable", true);
    converter.configure(props, true);
    byte[] converted = converter.fromConnectData(TOPIC, null, null);
    assertNull(converted);
  }

  @Test
  public void nullValueToJson() {
    // This characterizes the production of tombstone messages when Json schemas is not enabled
    Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
    converter.configure(props, true);
    byte[] converted = converter.fromConnectData(TOPIC, null, null);
    assertNull(converted);
  }

  @Test
  public void mismatchSchemaJson() {
    // If we have mismatching schema info, we should properly convert to a DataException
    assertThrows(
        DataException.class, () -> converter.fromConnectData(TOPIC, Schema.FLOAT64_SCHEMA, true));
  }

  @Test
  public void noSchemaToConnect() {
    Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
    converter.configure(props, true);
    assertEquals(new SchemaAndValue(null, true), converter.toConnectData(TOPIC, "true".getBytes()));
  }

  @Test
  public void noSchemaToJson() {
    Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
    converter.configure(props, true);
    JsonNode converted = parse(converter.fromConnectData(TOPIC, null, true));
    assertTrue(converted.isBoolean());
    assertEquals(true, converted.booleanValue());
  }

  @Test
  public void testJsonSchemaCacheSizeFromConfigFile() throws URISyntaxException, IOException {
    URL url = getClass().getResource("/connect-test.properties");
    File propFile = new File(url.toURI());
    String workerPropsFile = propFile.getAbsolutePath();
    Map<String, String> workerProps =
        !workerPropsFile.isEmpty()
            ? Utils.propsToStringMap(Utils.loadProps(workerPropsFile))
            : Collections.<String, String>emptyMap();

    ValidateSchemaJsonConverter rc = new ValidateSchemaJsonConverter();
    rc.configure(workerProps, false);
  }

  private JsonNode parse(byte[] json) {
    try {
      return objectMapper.readTree(json);
    } catch (IOException e) {
      fail("IOException during JSON parse: " + e.getMessage());
      throw new RuntimeException("failed");
    }
  }

  private void assertStructSchemaEqual(Schema schema, Struct struct) {
    converter.fromConnectData(TOPIC, schema, struct);
    assertEquals(schema, struct.schema());
  }
}
