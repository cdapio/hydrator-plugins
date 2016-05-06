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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.Transform;
import co.cask.hydrator.common.test.MockEmitter;
import co.cask.hydrator.common.test.MockLookupProvider;
import co.cask.hydrator.common.test.MockTransformContext;
import org.junit.Assert;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Test case for {@link ValueMapper}.
 */

public class ValueMapperTest {

  private static final Lookup<String> TEST_LOOKUP = new Lookup<String>() {
    @Override
    public String lookup(String key) {
      return key;
    }

    @Override
    public Map<String, String> lookup(String... keys) {
      Map<String, String> result = new HashMap<>();
      for (String key : keys) {
        result.put(key, key);
      }
      return result;
    }

    @Override
    public Map<String, String> lookup(Set<String> keys) {
      Map<String, String> result = new HashMap<>();
      for (String key : keys) {
        result.put(key, key);
      }
      return result;
    }
  };

  private static final Schema SCHEMA =
          Schema.recordOf("record",
                  Schema.Field.of("booleanField", Schema.of(Schema.Type.STRING)),
                  Schema.Field.of("intField", Schema.of(Schema.Type.STRING)));


  private static final StructuredRecord RECORD1 = StructuredRecord.builder(SCHEMA)
          .set("booleanField", "true")
          .set("intField", "28").build();


  @Test
  public void testSimple() throws Exception {

    String mapping = "{\"mapping\" : [{\"sourceField\" : \"booleanField\",\"targetField\" : \"booleanFieldName\"," +
            "\"lookup\":{\"tables\":{\"designationlookup\":{\"type\":\"DATASET\"," +
            "\"datasetProperties\":{\"dataset_argument1\":\"foo\",\"dataset_argument2\":\"bar\"}}}}}]}";

    ValueMapper.Config config = new ValueMapper.Config(mapping);

    Transform<StructuredRecord, StructuredRecord> transform = new ValueMapper(config);
    transform.initialize(new MockTransformContext("somestage",
            new HashMap<String, String>(),
            new MockLookupProvider(TEST_LOOKUP)));
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(RECORD1, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Assert.assertEquals("true", output.get("booleanFieldName"));
    Assert.assertEquals("28", output.get("intField"));

  }

}
