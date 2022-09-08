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

package io.cdap.plugin.format.text;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.format.text.input.TextInputFormatProvider;
import org.junit.Assert;
import org.junit.Test;

public class TextInputFormatProviderTest {
  @Test
  public void testValidateNoOfFields() {
    TextInputFormatProvider.TextConfig textConfig = new TextInputFormatProvider.TextConfig("pathField");
    Schema schema = textConfig.getSchema();
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), schema);
    TextInputFormatProvider provider = new TextInputFormatProvider(textConfig);
    provider.validate(formatContext);
    Assert.assertTrue(formatContext.getFailureCollector().getValidationFailures().isEmpty());
  }

  @Test
  public void testSchemaAddingPathField() {
    TextInputFormatProvider.TextConfig textConfig = new TextInputFormatProvider.TextConfig("pathField");
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), textConfig.getSchema());
    TextInputFormatProvider provider = new TextInputFormatProvider(textConfig);
    Schema providerSchema = provider.getSchema(formatContext);
    Schema expected = Schema.recordOf("textfile",
                                      Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
                                      Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
                                      Schema.Field.of("pathField", Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(expected, providerSchema);
  }

  @Test
  public void testSchemaWithoutPathField() {
    TextInputFormatProvider.TextConfig textConfig = new TextInputFormatProvider.TextConfig();
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), textConfig.getSchema());
    TextInputFormatProvider provider = new TextInputFormatProvider(textConfig);
    Schema providerSchema = provider.getSchema(formatContext);
    Schema expected = Schema.recordOf("textfile",
                                      Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
                                      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(expected, providerSchema);
  }
}
