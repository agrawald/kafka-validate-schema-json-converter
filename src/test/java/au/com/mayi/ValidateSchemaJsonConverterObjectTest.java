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

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ValidateSchemaJsonConverterObjectTest {
  private static final String TOPIC = "topic";

  private final ValidateSchemaJsonConverter converter = new ValidateSchemaJsonConverter();

  @BeforeEach
  public void setUp() {
    converter.configure(Collections.emptyMap(), false);
  }

  @Test
  public void shouldPassValidSuccessEvent() {
    byte[] MapJson =
        "{\"eventData\":{\"correlationId\":\"c\",\"responseStatus\":\"success\",\"eventType\":\"ecePerformAssessmentResponse\",\"payload\":{\"assessmentCaseId\":\"c\"}}}"
            .getBytes();
    final Map<String, Object> expected = TestDataBuilder.getExpectedJson(false, false);
    final SchemaAndValue converted = converter.toConnectData(TOPIC, MapJson);
    assertNull(converted.schema());
    assertEquals(expected, converted.value());
  }

  @Test
  public void shouldPassValidBusinessErrorEvent() {
    byte[] MapJson =
        "{\"eventData\":{\"correlationId\":\"c\",\"responseStatus\":\"businessError\",\"eventType\":\"ecePerformAssessmentResponse\",\"payload\":{\"exceptions\":[{\"exceptionType\":\"i\",\"exceptionCategory\":\"e\",\"exceptionCode\":\"e\",\"exceptionSource\":\"l\",\"exceptionDescription\":\"d\",\"exceptionOtherData\":\"e\"}]}}}}"
            .getBytes();
    final Map<String, Object> expected = TestDataBuilder.getExpectedJson(true, false);
    final SchemaAndValue converted = converter.toConnectData(TOPIC, MapJson);
    assertNull(converted.schema());
    assertEquals(expected, converted.value());
  }

  @Test
  public void shouldPassValidSystemErrorEvent() {
    byte[] MapJson =
        "{\"eventData\":{\"correlationId\":\"c\",\"responseStatus\":\"systemError\",\"eventType\":\"ecePerformAssessmentResponse\",\"payload\":{\"exceptions\":[{\"exceptionType\":\"i\",\"exceptionCategory\":\"e\",\"exceptionCode\":\"e\",\"exceptionSource\":\"l\",\"exceptionDescription\":\"d\",\"exceptionOtherData\":\"e\"}]}}}}"
            .getBytes();
    final Map<String, Object> expected = TestDataBuilder.getExpectedJson(false, true);
    final SchemaAndValue converted = converter.toConnectData(TOPIC, MapJson);
    assertNull(converted.schema());
    assertEquals(expected, converted.value());
  }

  @Test
  public void shouldFailWhenInvalidEventType() {
    byte[] MapJson =
        "{\"eventData\":{\"correlationId\":\"c\",\"responseStatus\":\"success\",\"eventType\":\"performAssessmentResponse\",\"payload\":{\"assessmentCaseId\":\"c\"}}}"
            .getBytes();

    assertThrows(DataException.class, () -> converter.toConnectData(TOPIC, MapJson));
  }

  @Test
  public void shouldFailWhenInvalidResponseStatus() {
    byte[] MapJson =
        "{\"eventData\":{\"correlationId\":\"c\",\"responseStatus\":\"invalidStatus\",\"eventType\":\"ecePerformAssessmentResponse\",\"payload\":{\"assessmentCaseId\":\"c\"}}}"
            .getBytes();

    assertThrows(DataException.class, () -> converter.toConnectData(TOPIC, MapJson));
  }

  @Test
  public void shouldFailInvalidJsonEvent() {
    byte[] MapJson =
        "{\"eventData\":{\"correlationId\":\"c\",\"responseStatus\":\"e\",\"eventType\":\"e\",\"payload\":{\"assessmentCaseId\":\"c\"}}"
            .getBytes();
    assertThrows(DataException.class, () -> converter.toConnectData(TOPIC, MapJson));
  }

  @Test
  public void shouldFailWhenRequiredFieldsMissing() {
    byte[] MapJson =
        "{\"eventData\":{\"correlationId\":\"c\",\"responseStatus\":\"e\",\"payload\":{\"assessmentCaseId\":\"c\"}}"
            .getBytes();
    assertThrows(DataException.class, () -> converter.toConnectData(TOPIC, MapJson));
  }

  private static class TestDataBuilder {
    static List<Map<String, Object>> getExceptions() {
      final Map<String, Object> map =
          new HashMap<String, Object>() {
            {
              put("exceptionType", "i");
              put("exceptionCategory", "e");
              put("exceptionCode", "e");
              put("exceptionSource", "l");
              put("exceptionDescription", "d");
              put("exceptionOtherData", "e");
            }
          };

      return Collections.singletonList(map);
    }

    static Map<String, Object> getPayload(boolean isError) {
      if (isError) {
        return new HashMap<String, Object>() {
          {
            put("exceptions", getExceptions());
          }
        };
      }
      return new HashMap<String, Object>() {
        {
          put("assessmentCaseId", "c");
        }
      };
    }

    static Map<String, Object> getEventData(boolean isBusinessError, boolean isSystemError) {
      return new HashMap<String, Object>() {
        {
          put("correlationId", "c");
          put("eventType", "ecePerformAssessmentResponse");
          put(
              "responseStatus",
              isBusinessError ? "businessError" : isSystemError ? "systemError" : "success");
          put("payload", getPayload(isBusinessError || isSystemError));
        }
      };
    }

    static Map<String, Object> getExpectedJson(boolean isBusinessError, boolean isSystemError) {
      return new HashMap<String, Object>() {
        {
          put("eventData", getEventData(isBusinessError, isSystemError));
        }
      };
    }
  }
}
