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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link BatchSink} that verifies referenceName property
 *
 * @param <IN> the type of input object to the sink
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class ReferenceBatchSink<IN, KEY_OUT, VAL_OUT> extends BatchSink<IN, KEY_OUT, VAL_OUT> {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceBatchSink.class);

  private final ReferencePluginConfig config;

  public ReferenceBatchSink(ReferencePluginConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    IdUtils.validateReferenceName(config.referenceName, collector);
    collector.getOrThrowException();
  }

  /**
   * Record field-level lineage for batch sink plugins (WriteOperation). This method should be called from prepareRun of
   * any batch sink plugin.
   *
   * @param context BatchSinkContext from prepareRun
   * @param outputName name of output dataset
   * @param tableSchema schema of fields. Also used to determine list of field names. Schema and schema.getFields() must
   * not be null.
   * @param operationName name of the operation
   * @param description operation description; complete sentences preferred
   */
  protected void recordLineage(BatchSinkContext context, String outputName, Schema tableSchema, String operationName,
                               String description) {
    if (tableSchema == null) {
      LOG.warn("Schema for output %s is null. Field-level lineage will not be recorded.", outputName);
      return;
    }
    if (tableSchema.getFields() == null) {
      LOG.warn("Schema fields for output %s is empty. Field-level lineage will not be recorded", outputName);
      return;
    }
    LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
    lineageRecorder.createExternalDataset(tableSchema);
    List<String> fieldNames = tableSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
    if (!fieldNames.isEmpty()) {
      lineageRecorder.recordWrite(operationName, description, fieldNames);
    }
  }
}
