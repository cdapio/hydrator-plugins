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

package co.cask.hydrator.plugin.transform;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.mock.common.MockEmitter;
import co.cask.cdap.etl.mock.transform.MockTransformContext;
import org.junit.Test;

/**
 * Created by Abhinav on 12/2/16.
 */
public class RulesTransformTest {

  private static final Schema SIMPLE_TYPES_SCHEMA =
    Schema.recordOf("record",
                    Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)));

  @Test
  public void testRules() throws Exception {
    RulesTransform.RulesTranformConfig rulesTranformConfig = new RulesTransform.RulesTranformConfig("stringField",
                                                                                                    "contains:x");
    Transform<StructuredRecord, StructuredRecord> transform = new RulesTransform(rulesTranformConfig);
    TransformContext transformContext = new MockTransformContext();
    transform.initialize(transformContext);

    StructuredRecord input = StructuredRecord.builder(SIMPLE_TYPES_SCHEMA)
      .set("intField", 1)
      .set("stringField", "abxy")
      .build();

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(input, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);
  }
}
