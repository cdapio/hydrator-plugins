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

package co.cask.hydrator.plugin.batch.aggregator;

import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for {@link RowDenormalizerConfig}.
 */
public class RowDenormalizerConfigTest {
  Schema outputSchema = Schema.recordOf(
    "denormalizedRecord",
    Schema.Field.of("KeyField", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Firstname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("lname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("addr", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
  );

  @Test
  public void testDenormalizerConfig() {
    RowDenormalizerConfig config = new RowDenormalizerConfig("KeyField", "FieldName", "FieldValue", "Firstname:," +
      "Lastname:lname," + "Address:addr", outputSchema.toString());

    Assert.assertEquals(ImmutableList.of("Firstname", "lname", "addr"), config.getOutputFields());
    Assert.assertEquals(ImmutableMap.of("Firstname", RowDenormalizerConfig.CHECK_ALIAS, "Lastname", "lname",
                                        "Address", "addr"), config.getOutputFieldMappings());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDormalizerConfWithNoKeyField() {
    RowDenormalizerConfig config = new RowDenormalizerConfig("", "FieldName", "FieldValue", "Firstname:," +
      "Lastname:lname," + "Address:addr", outputSchema.toString());
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDormalizerConfWithNoFieldName() {
    RowDenormalizerConfig config = new RowDenormalizerConfig("KeyField", "", "FieldValue", "Firstname:," +
      "Lastname:lname," + "Address:addr", outputSchema.toString());
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDormalizerConfWithNoFieldValue() {
    RowDenormalizerConfig config = new RowDenormalizerConfig("KeyField", "FieldName", "", "Firstname:," +
      "Lastname:lname," + "Address:addr", outputSchema.toString());
    config.validate();
  }
}




