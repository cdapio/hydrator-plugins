/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;

import javax.annotation.Nullable;

/**
 * Config class for Cube dataset sinks
 */
public class CubeSinkConfig extends BatchReadableWritableConfig {

  @Name(Properties.Cube.DATASET_RESOLUTIONS)
  @Description("Aggregation resolutions to be used if a new Cube dataset " +
    "needs to be created. See Cube dataset configuration details available at " +
    "http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/cube.html#cube-configuration " +
    "for more information.")
  @Nullable
  String resolutions;

  @Name(Properties.Cube.AGGREGATIONS)
  @Description("Provided as a collection of aggregation-groups. " +
    "Each aggregation group is identified by a unique name and value is a collection of fields. " +
    "Example, if aggregations are desired on fields 'abc' and 'xyz', " +
    "the property can have aggregation group with one entry, with entry's key 'agg1' and " +
    "value 'abc,xyz'. the fields are comma separated, key and value are delimited by ':' and " +
    "the entries are delimited by ';'. See [Cube dataset configuration details] for more information. " +
    "[Cube dataset configuration details]: " +
    "http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/cube.html")
  @Nullable
  String aggregations;

  @Name(Properties.Cube.DATASET_OTHER)
  @Description("Extra dataset properties to be included")
  @Nullable
  String datasetOther;

  @Name(Properties.Cube.FACT_TS_FIELD)
  @Description("Name of the StructuredRecord field that contains the timestamp to be used in the CubeFact. " +
    "If not provided, the current time of the record processing will be used as the CubeFact timestamp.")
  @Nullable
  String tsField;

  @Name(Properties.Cube.FACT_TS_FORMAT)
  @Description("Format of the value of timestamp field; " +
    "example: \"HH:mm:ss\" (used if " + Properties.Cube.FACT_TS_FIELD + " is provided).")
  @Nullable
  String tsFormat;

  @Name(Properties.Cube.MEASUREMENTS)
  @Description("Measurements to be extracted from StructuredRecord to be used" +
    "in CubeFact. Supports collection of measurements and requires at least one measurement to be provided." +
    "Each measurement has measurement-name and measurement-type. " +
    "Currently supported measurement types are COUNTER, GAUGE." +
    "For example, to use the 'price' field as a measurement of type gauge, and the '" +
    "quantity' field as a measurement of type counter, property will have two measurements. " +
    "one measurement with name 'price' and type 'GAUGE', second measurement with name 'quantity' and type 'COUNTER'. " +
    "the key and value are delimited by ':' while the entries are delimited by ';'")
  String measurements;

  public CubeSinkConfig(String name, String resolutions, String aggregations,
                        String tsField, String tsFormat, String measurements) {
    super(name);
    this.resolutions = resolutions;
    this.aggregations = aggregations;
    this.tsField = tsField;
    this.tsFormat = tsFormat;
    this.measurements = measurements;
  }

  @Nullable
  public String getResolutions() {
    return resolutions;
  }

  @Nullable
  public String getAggregations() {
    return aggregations;
  }

  @Nullable
  public String getTsField() {
    return tsField;
  }

  @Nullable
  public String getTsFormat() {
    return tsFormat;
  }

  @Nullable
  public String getMeasurements() {
    return measurements;
  }
}

