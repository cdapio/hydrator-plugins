/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.hydrator.plugin.spark;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Splitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Unit test for {@link SparkUtils}
 */
public class SparkUtilsTest {
  @Test
  public void testGetStringInputField() throws Exception {
    Schema input = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                                   Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    StructuredRecord record = StructuredRecord.builder(input).set("offset", 1).set("body", "Hi heard about Spark")
      .build();
    Splitter splitter = Splitter.on(Pattern.compile(" "));
    List<String> expected = new ArrayList<>();
    expected.add("Hi");
    expected.add("heard");
    expected.add("about");
    expected.add("Spark");
    Assert.assertEquals(expected, SparkUtils.getInputFieldValue(record, "body", splitter));
    record = StructuredRecord.builder(input).set("offset", 2).set("body", "Classes in Java").build();
    expected = new ArrayList<>();
    expected.add("Classes");
    expected.add("in");
    expected.add("Java");
    Assert.assertEquals(expected, SparkUtils.getInputFieldValue(record, "body", splitter));
  }

  @Test
  public void testGetArrayInputField() throws Exception {
    Schema input = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                                   Schema.Field.of("body", Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    StructuredRecord record = StructuredRecord.builder(input).set("offset", 1)
      .set("body", new String[]{"Hi", "heard", "about", "Spark"}).build();
    Splitter splitter = Splitter.on(Pattern.compile(" "));
    List<String> expected = new ArrayList<>();
    expected.add("Hi");
    expected.add("heard");
    expected.add("about");
    expected.add("Spark");
    Assert.assertEquals(expected, SparkUtils.getInputFieldValue(record, "body", splitter));
    record = StructuredRecord.builder(input).set("offset", 2).set("body", new String[]{"Classes", "in", "Java"})
      .build();
    expected = new ArrayList<>();
    expected.add("Classes");
    expected.add("in");
    expected.add("Java");
    Assert.assertEquals(expected, SparkUtils.getInputFieldValue(record, "body", splitter));
  }

  @Test
  public void testSplitterGetInputFieldValue() throws Exception {
    Schema input = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                                   Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    StructuredRecord record = StructuredRecord.builder(input).set("offset", 1).set("body", "Hi heard about Spark")
      .build();
    Splitter splitter = Splitter.on(Pattern.compile(":"));
    List<String> expected = new ArrayList<>();
    expected.add("Hi heard about Spark");
    Assert.assertEquals(expected, SparkUtils.getInputFieldValue(record, "body", splitter));
    record = StructuredRecord.builder(input).set("offset", 2).set("body", "Classes in Java").build();
    expected = new ArrayList<>();
    expected.add("Classes in Java");
    Assert.assertEquals(expected, SparkUtils.getInputFieldValue(record, "body", splitter));
  }

  @Test
  public void testWhitespaceSplitterGetInputFieldValue() throws Exception {
    Schema input = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                                   Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    StructuredRecord record = StructuredRecord.builder(input).set("offset", 1).set("body", "Hi heard about   Spark")
      .build();
    Splitter splitter = Splitter.on(Pattern.compile("\\s+"));
    List<String> expected = new ArrayList<>();
    expected.add("Hi");
    expected.add("heard");
    expected.add("about");
    expected.add("Spark");
    Assert.assertEquals(expected, SparkUtils.getInputFieldValue(record, "body", splitter));
    record = StructuredRecord.builder(input).set("offset", 2).set("body", "Classes   in Java").build();
    expected = new ArrayList<>();
    expected.add("Classes");
    expected.add("in");
    expected.add("Java");
    Assert.assertEquals(expected, SparkUtils.getInputFieldValue(record, "body", splitter));
  }

  @Test
  public void testInvalidInputField() throws Exception {
    Schema input = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.STRING)),
                                   Schema.Field.of("body", Schema.arrayOf(Schema.of(Schema.Type.INT))));
    StructuredRecord record = StructuredRecord.builder(input).set("offset", 1)
      .set("body", new int[]{1, 2, 3}).build();
    Splitter splitter = Splitter.on(Pattern.compile(" "));
    try {
      SparkUtils.getInputFieldValue(record, "offset", splitter);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Schema type mismatch for field offset. Please make sure the value to be used for feature " +
                            "generation is an array of string or a string.", e.getMessage());
    }
  }
}
