/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.format.delimited.input;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.format.SchemaDetector;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;

/**
 * Tests for {@link DelimitedInputFormatProvider}
 */
public class DelimitedInputFormatProviderTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCSVSchemaDetection() throws IOException {
    testSchemaDetection(",", new CSVInputFormatProvider(new DelimitedConfig()));
  }

  @Test
  public void testTSVSchemaDetection() throws IOException {
    testSchemaDetection("\t", new TSVInputFormatProvider(new DelimitedConfig()));
  }

  @Test
  public void testDelimitedSchemaDetection() throws IOException {
    testSchemaDetection("|", new DelimitedInputFormatProvider(new DelimitedInputFormatProvider.Conf("|")));
  }

  private void testSchemaDetection(String delimiter, ValidatingInputFormat inputFormat) throws IOException {
    Schema schema = getSchema(inputFormat, delimiter);
    Schema expected = Schema.recordOf("text",
                                      Schema.Field.of("body_0", Schema.of(Schema.Type.STRING)),
                                      Schema.Field.of("body_1", Schema.of(Schema.Type.STRING)),
                                      Schema.Field.of("body_2", Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(expected, schema);
  }

  private Schema getSchema(ValidatingInputFormat inputFormat, String delimiter) throws IOException {
    String content = String.format("a%sb%sc", delimiter, delimiter);
    File file = TMP_FOLDER.newFile(UUID.randomUUID() + ".txt");
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(content.getBytes(StandardCharsets.UTF_8));
    }
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), null);
    SchemaDetector schemaDetector = new SchemaDetector(inputFormat);
    Schema schema = schemaDetector.detectSchema(file.getAbsolutePath(), formatContext, Collections.emptyMap());
    return schema;
  }

  @Test
  public void testAddPathField() throws IOException {
    String delimiter = "|";
    Schema schema = getSchema(new DelimitedInputFormatProvider(new
      DelimitedInputFormatProvider.Conf(delimiter, "pathField")), delimiter);
    Schema expected = Schema.recordOf("text",
                                      Schema.Field.of("body_0", Schema.of(Schema.Type.STRING)),
                                      Schema.Field.of("body_1", Schema.of(Schema.Type.STRING)),
                                      Schema.Field.of("body_2", Schema.of(Schema.Type.STRING)),
                                      Schema.Field.of("pathField", Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(expected, schema);
  }
}
