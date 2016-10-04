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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Config class for Trainers. Contains common config properties and validation methods to be used in trainers.
 */
public class MLTrainerConfig extends PluginConfig {

  @Description("The name of the FileSet to save the model to.")
  protected String fileSetName;

  @Description("Path of the FileSet to save the model to.")
  protected String path;

  @Nullable
  @Description("A comma-separated sequence of fields that needs to be used for training.")
  protected String featureFieldsToInclude;

  @Nullable
  @Description("A comma-separated sequence of fields that needs to be excluded from being used in training.")
  protected String featureFieldsToExclude;

  @Description("The field from which to get the label. It must be of type double.")
  protected String labelField;

  public MLTrainerConfig() {
  }

  public MLTrainerConfig(String fileSetName, String path, @Nullable String featureFieldsToInclude,
                         @Nullable String featureFieldsToExclude, String labelField) {
    this.fileSetName = fileSetName;
    this.path = path;
    this.featureFieldsToInclude = featureFieldsToInclude;
    this.featureFieldsToExclude = featureFieldsToExclude;
    this.labelField = labelField;
  }

  /**
   * Validate the config parameters for the spark sink class.
   *
   * @param inputSchema       schema of the received record.
   * @param featuresToInclude features to be used for training.
   * @param featuresToExclude features to be excluded when training.
   * @param labelField   field containing the label values.
   */
  public static void validate(Schema inputSchema, @Nullable String featuresToInclude,
                                       @Nullable String featuresToExclude, String labelField,
                                       @Nullable String cardinalityMapping) {
    SparkUtils.validateConfigParameters(inputSchema, featuresToInclude, featuresToExclude, labelField,
                                        cardinalityMapping);
    SparkUtils.validateLabelFieldForTrainer(inputSchema, labelField);
  }
}
