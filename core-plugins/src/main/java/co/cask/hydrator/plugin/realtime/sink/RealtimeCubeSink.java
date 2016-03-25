/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.realtime.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.hydrator.plugin.common.CubeSinkConfig;
import co.cask.hydrator.plugin.common.CubeUtils;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.common.StructuredRecordToCubeFact;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link RealtimeSink} that writes data to a {@link Cube} dataset.
 * <p/>
 * This {@link RealtimeCubeSink} takes a {@link co.cask.cdap.api.data.format.StructuredRecord} in, maps it to a
 * {@link co.cask.cdap.api.dataset.lib.cube.CubeFact}, and writes it to a {@link Cube} dataset identified by the
 * {@link Properties.Cube#DATASET_NAME} property.
 * <p/>
 * If {@link Cube} dataset does not exist, it will be created using properties provided with this sink. See more
 * information on available {@link Cube} dataset configuration properties at
 * {@link co.cask.cdap.data2.dataset2.lib.cube.CubeDatasetDefinition}.
 * <p/>
 * To configure transformation from a {@link co.cask.cdap.api.data.format.StructuredRecord} to a
 * {@link co.cask.cdap.api.dataset.lib.cube.CubeFact}, the
 * mapping configuration is required, following {@link StructuredRecordToCubeFact} documentation.
 */
@Plugin(type = "realtimesink")
@Name("Cube")
@Description("Real-time sink that writes data to a CDAP Cube dataset.")
public class RealtimeCubeSink extends RealtimeSink<StructuredRecord> {
  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final CubeSinkConfig config;

  public RealtimeCubeSink(CubeSinkConfig config) {
    this.config = config;
  }

  private StructuredRecordToCubeFact transform;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    String datasetName = config.getName();
    Preconditions.checkArgument(datasetName != null && !datasetName.isEmpty(), "Dataset name must be given.");

    pipelineConfigurer.createDataset(datasetName, Cube.class.getName(), DatasetProperties.builder()
      .addAll(getProperties())
      .build());
  }

  protected Map<String, String> getProperties() {
    Map<String, String> properties = new HashMap<>(config.getProperties().getProperties());
    // add aggregations
    if (!Strings.isNullOrEmpty(config.getAggregations())) {
      properties.remove(Properties.Cube.AGGREGATIONS);
      properties.putAll(CubeUtils.parseAndGetProperties(Properties.Cube.AGGREGATIONS,
                                                        config.getAggregations(), ";", ":",
                                                        new Function<String, String>() {
                                                          @Override
                                                          public String apply(String input) {
                                                            return "dataset.cube.aggregation." + input + ".dimensions";
                                                          }
                                                        }));
    }

    // add measurements
    if (!Strings.isNullOrEmpty(config.getMeasurements())) {
      properties.remove(Properties.Cube.MEASUREMENTS);
      properties.putAll(CubeUtils.parseAndGetProperties(Properties.Cube.MEASUREMENTS,
                                                        config.getMeasurements(), ";", ":",
                                                        new Function<String, String>() {
                                                          @Override
                                                          public String apply(String input) {
                                                            return Properties.Cube.MEASUREMENT_PREFIX + input;
                                                          }
                                                        }));
    }
    return properties;
  }

  @Override
  public int write(Iterable<StructuredRecord> objects, DataWriter dataWriter) throws Exception {
    Cube cube = dataWriter.getDataset(config.getName());
    int count = 0;
    for (StructuredRecord record : objects) {
      cube.add(transform.transform(record));
      count++;
    }
    return count;
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    transform = new StructuredRecordToCubeFact(getProperties());
  }
}
