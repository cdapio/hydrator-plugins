/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.common;

import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for SchemaValidator
 */
public class SchemaValidatorTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private static Schema schema;
  private static String validDate;
  private static String inValidDate;

  @BeforeClass
  public static void init() {
    schema = Schema.recordOf("record",
                             Schema.Field.of("simplefield", Schema.of(Schema.LogicalType.DATETIME)),
                             Schema.Field
                               .of("unionfield", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))),
                             Schema.Field
                               .of("arrayfield", Schema.arrayOf(Schema.of(Schema.LogicalType.DATETIME))),
                             Schema.Field.of("mapfield1", Schema
                               .mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.LogicalType.DATETIME))),
                             Schema.Field.of("mapfield2", Schema
                               .mapOf(Schema.of(Schema.LogicalType.DATETIME), Schema.of(Schema.Type.STRING))),
                             Schema.Field.of("stringfield", Schema.of(Schema.Type.STRING)));
    validDate = LocalDateTime.now().toString();
    //not in ISO 8601 format
    inValidDate = "2020-10-01 10:10:10";
  }

  @Test
  public void testValidateDateTimeFieldSimple() {
    //no exception
    SchemaValidator.validateDateTimeField(schema.getField("simplefield").getSchema(), "testdate", validDate);
    expectedException.expect(UnexpectedFormatException.class);
    SchemaValidator.validateDateTimeField(schema.getField("simplefield").getSchema(), "testdate", inValidDate);
  }

  @Test
  public void testValidateDateTimeFieldUnion() {
    //no exception
    SchemaValidator.validateDateTimeField(schema.getField("unionfield").getSchema(), "unionfield", validDate);
    expectedException.expect(UnexpectedFormatException.class);
    SchemaValidator.validateDateTimeField(schema.getField("unionfield").getSchema(), "unionfield", inValidDate);
  }

  @Test
  public void testValidateDateTimeFieldArray() {
    //no exception
    SchemaValidator
      .validateDateTimeField(schema.getField("arrayfield").getSchema(), "arrayfield", new String[]{validDate});
    expectedException.expect(UnexpectedFormatException.class);
    SchemaValidator
      .validateDateTimeField(schema.getField("arrayfield").getSchema(), "arrayfield",
                             Collections.singletonList(inValidDate));
  }

  @Test
  public void testValidateDateTimeFieldMap() {
    //no exception
    Map<String, String> testMap = new HashMap<>();
    testMap.put("test", validDate);
    SchemaValidator
      .validateDateTimeField(schema.getField("mapfield1").getSchema(), "mapfield1", testMap);
    testMap.put("test", inValidDate);
    expectedException.expect(UnexpectedFormatException.class);
    SchemaValidator
      .validateDateTimeField(schema.getField("mapfield1").getSchema(), "mapfield1",
                             testMap);
  }

  @Test
  public void testValidateDateTimeFieldMap1() {
    //no exception
    Map<String, String> testMap = new HashMap<>();
    testMap.put(validDate, "test");
    SchemaValidator
      .validateDateTimeField(schema.getField("mapfield2").getSchema(), "mapfield2", testMap);
    testMap.put(inValidDate, "test");
    expectedException.expect(UnexpectedFormatException.class);
    SchemaValidator
      .validateDateTimeField(schema.getField("mapfield2").getSchema(), "mapfield2",
                             testMap);
  }

  @Test
  public void testValidateDateTimeFieldString() {
    //no exception for both valid and invalid
    SchemaValidator.validateDateTimeField(schema.getField("stringfield").getSchema(), "stringfield", validDate);
    SchemaValidator.validateDateTimeField(schema.getField("stringfield").getSchema(), "stringfield", inValidDate);
  }
}
