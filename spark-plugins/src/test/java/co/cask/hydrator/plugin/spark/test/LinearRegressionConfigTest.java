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
package co.cask.hydrator.plugin.spark.test;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.spark.LinearRegressionPredictor;
import co.cask.hydrator.plugin.spark.LinearRegressionTrainer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for LinearRegressionTrainerConfig and LinearRegressionPredictorConfig classes.
 */
public class LinearRegressionConfigTest {
  private final Schema schema = Schema.recordOf("input-record", Schema.Field.of("age", Schema.of(Schema.Type.INT)),
                                                Schema.Field.of("height", Schema.of(Schema.Type.DOUBLE)),
                                                Schema.Field.of("smoke", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("gender", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("lung_capacity",
                                                                Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

  @Test
  public void testInvalidLabelField() throws Exception {
    LinearRegressionTrainer.LinearRegressionTrainerConfig config =
      new LinearRegressionTrainer.LinearRegressionTrainerConfig("linear-regression-model", "linearRegression", "age",
                                                                null, "wrong_label", 100, 1.0);
    try {
      config.validate(schema);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Label field wrong_label does not exists in the input schema.", e.getMessage());
    }
  }

  @Test
  public void testInvalidFeatures() throws Exception {
    LinearRegressionTrainer.LinearRegressionTrainerConfig config =
      new LinearRegressionTrainer.LinearRegressionTrainerConfig("linear-regression-model", "linearRegression",
                                                                "age,width", null, "lung_capacity", 100, 1.0);
    try {
      config.validate(schema);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Field width does not exists in the input schema.", e.getMessage());
    }
  }

  @Test
  public void testIncludeAllFeatures() throws Exception {
    LinearRegressionTrainer.LinearRegressionTrainerConfig config =
      new LinearRegressionTrainer.LinearRegressionTrainerConfig("linear-regression-model", "linearRegression",
                                                                null, null, "lung_capacity", 100, 1.0);
    try {
      config.validate(schema);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Features must be of type : int, double, float, long but was of type STRING for " +
                            "field gender.", e.getMessage());
    }
  }

  @Test
  public void testLabelFieldTrainer() throws Exception {
    LinearRegressionTrainer.LinearRegressionTrainerConfig config =
      new LinearRegressionTrainer.LinearRegressionTrainerConfig("linear-regression-model", "linearRegression", "age",
                                                                null, "gender", 100, 1.0);
    try {
      config.validate(schema);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Label field must be of type Double, but was STRING.", e.getMessage());
    }
  }

  @Test
  public void testPredictionFieldPredictor() throws Exception {
    LinearRegressionPredictor.LinearRegressionPredictorConfig config =
      new LinearRegressionPredictor.LinearRegressionPredictorConfig("linear-regression-model", "linearRegression",
                                                                    "age", null, "lung_capacity");
    try {
      config.validate(schema);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Prediction field must not already exist in the input schema.", e.getMessage());
    }
  }
}
