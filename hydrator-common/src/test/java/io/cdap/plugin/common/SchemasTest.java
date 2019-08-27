/*
 * Copyright © 2019 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link Schemas}.
 */
public class SchemasTest {

  @Test
  public void test() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("bytes", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("decimal", Schema.decimalOf(10, 5)),
                                    Schema.Field.of("ts_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("ts_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("time_micros", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                    Schema.Field.of("time_millis", Schema.of(Schema.LogicalType.TIME_MILLIS)),
                                    Schema.Field.of("map",
                                                    Schema.nullableOf(Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                                   Schema.of(Schema.Type.INT)))),
                                    Schema.Field.of("union", Schema.unionOf(Schema.of(Schema.Type.STRING))));

    Assert.assertEquals("bytes", Schemas.getDisplayName(schema.getField("bytes").getSchema()));
    Assert.assertEquals("string", Schemas.getDisplayName(schema.getField("string").getSchema()));
    Assert.assertEquals("date", Schemas.getDisplayName(schema.getField("date").getSchema()));
    Assert.assertEquals("decimal", Schemas.getDisplayName(schema.getField("decimal").getSchema()));
    Assert.assertEquals("timestamp", Schemas.getDisplayName(schema.getField("ts_micros").getSchema()));
    Assert.assertEquals("long", Schemas.getDisplayName(schema.getField("ts_millis").getSchema()));
    Assert.assertEquals("time", Schemas.getDisplayName(schema.getField("time_micros").getSchema()));
    Assert.assertEquals("int", Schemas.getDisplayName(schema.getField("time_millis").getSchema()));
    Assert.assertEquals("map", Schemas.getDisplayName(schema.getField("map").getSchema()));
    Assert.assertEquals("union", Schemas.getDisplayName(schema.getField("union").getSchema()));
  }
}
