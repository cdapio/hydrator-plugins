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

package io.cdap.plugin.format.blob.input;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

public class BlobInputFormatProviderTest {
  @Test
  public void testValidate() {
    BlobInputFormatProvider.BlobConfig blobConfig = new BlobInputFormatProvider.BlobConfig("pathField");
    Schema schema = blobConfig.getSchema();
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), schema);
    BlobInputFormatProvider provider = new BlobInputFormatProvider(blobConfig);
    provider.validate(formatContext);
    Assert.assertTrue(formatContext.getFailureCollector().getValidationFailures().isEmpty());
  }

  @Test
  public void testSchemaAddingPathField() {
    BlobInputFormatProvider.BlobConfig blobConfig = new BlobInputFormatProvider.BlobConfig("pathField");
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), blobConfig.getSchema());
    BlobInputFormatProvider provider = new BlobInputFormatProvider(blobConfig);
    Schema providerSchema = provider.getSchema(formatContext);
    Schema expected = Schema.recordOf("blob",
                                      Schema.Field.of("body", Schema.of(Schema.Type.BYTES)),
                                      Schema.Field.of("pathField", Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(expected, providerSchema);
  }

  @Test
  public void testSchemaWithoutPathField() {
    BlobInputFormatProvider.BlobConfig blobConfig = new BlobInputFormatProvider.BlobConfig();
    FormatContext formatContext = new FormatContext(new MockFailureCollector(), blobConfig.getSchema());
    BlobInputFormatProvider provider = new BlobInputFormatProvider(blobConfig);
    Schema providerSchema = provider.getSchema(formatContext);
    Schema expected = Schema.recordOf("blob",
                                       Schema.Field.of("body", Schema.of(Schema.Type.BYTES)));
    Assert.assertEquals(expected, providerSchema);
  }
}
