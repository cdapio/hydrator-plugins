/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

/**
 * Class that contains common names and description used across plugins.
 */
public final class Constants {

  /**
   * Common Reference Name property name and description
   */
  public static class Reference {
    public static final String REFERENCE_NAME = "referenceName";
    public static final String REFERENCE_NAME_DESCRIPTION = "This will be used to uniquely identify this source/sink " +
      "for lineage, annotating metadata, etc.";
    public static final String FQN = "fqn";
    public static final String LOCATION = "location";
  }

  public static final String EXTERNAL_DATASET_TYPE = "externalDataset";
  public static final String MARKER = "marker";

  private Constants() {
  }
}
