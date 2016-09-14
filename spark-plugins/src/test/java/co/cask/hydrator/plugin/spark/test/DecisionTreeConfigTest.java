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
import co.cask.hydrator.plugin.spark.DecisionTreePredictor;
import co.cask.hydrator.plugin.spark.DecisionTreeTrainer;
import org.junit.Assert;
import org.junit.Test;

public class DecisionTreeConfigTest {
  private final Schema schema =
    Schema.recordOf("flightData", Schema.Field.of("dofM", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("dofW", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("carrier", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("tailNum", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("flightNum", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("originId", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("origin", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("destId", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("dest", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("scheduleDepTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("deptime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("depDelayMins", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("scheduledArrTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("arrTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("arrDelay", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("elapsedTime", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("distance", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("delayed", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

  @Test
  public void testInvalidPredictionField() throws Exception {
    DecisionTreeTrainer.DecisionTreeTrainerConfig config =
      new DecisionTreeTrainer.DecisionTreeTrainerConfig("decision-tree-regression-model", "decisionTreeRegression",
                                                        "dofM,dofW,scheduleDepTime,scheduledArrTime,carrier," +
                                                          "elapsedTime,originId,destId", null, null, "dealyed", 100, 9);
    try {
      config.validate(schema);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Label field dealyed does not exists in the input schema.", e.getMessage());
    }
  }

  @Test
  public void testInvalidFeatures() throws Exception {
    DecisionTreeTrainer.DecisionTreeTrainerConfig config =
      new DecisionTreeTrainer.DecisionTreeTrainerConfig("decision-tree-regression-model", "decisionTreeRegression",
                                                        "dofM,dofW,scheduleDepTime,scheduledArrTime,carrier," +
                                                          "elapsedTime,originId,destinationId", null, null, "delayed",
                                                        100, 9);
    try {
      config.validate(schema);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Field destinationId does not exists in the input schema.", e.getMessage());
    }
  }

  @Test
  public void testIncludeAllFeatures() throws Exception {
    DecisionTreeTrainer.DecisionTreeTrainerConfig config =
      new DecisionTreeTrainer.DecisionTreeTrainerConfig("decision-tree-regression-model", "decisionTreeRegression",
                                                        null, null, null, "delayed", 100, 9);
    try {
      config.validate(schema);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Field to classify must be of type : int, double, float, long but was of type STRING for " +
                            "field tailNum.", e.getMessage());
    }
  }

  @Test
  public void testPredictionFieldTrainer() throws Exception {
    DecisionTreeTrainer.DecisionTreeTrainerConfig config =
      new DecisionTreeTrainer.DecisionTreeTrainerConfig("decision-tree-regression-model", "decisionTreeRegression",
                                                        "dofM,dofW,scheduleDepTime,scheduledArrTime,carrier," +
                                                          "elapsedTime,originId,destId", null, null, "tailNum", 100, 9);
    try {
      config.validate(schema);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Label field must be of type Double, but was STRING.", e.getMessage());
    }
  }

  @Test
  public void testPredictionFieldPredictor() throws Exception {
    DecisionTreePredictor.DecisionTreePredictorConfig config =
      new DecisionTreePredictor.DecisionTreePredictorConfig("decision-tree-regression-model", "decisionTreeRegression",
                                                            "dofM,dofW,scheduleDepTime,scheduledArrTime,carrier," +
                                                              "elapsedTime,originId,destId", null, "delayed");
    try {
      config.validate(schema);
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Prediction field must not already exist in the input schema.", e.getMessage());
    }
  }
}
