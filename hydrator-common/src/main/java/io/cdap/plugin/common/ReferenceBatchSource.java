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
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link BatchSource} that verifies referenceName property
 *
 * @param <KEY_IN> the type of input key from the Batch run
 * @param <VAL_IN> the type of input value from the Batch run
 * @param <OUT> the type of output for the source
 */
public abstract class ReferenceBatchSource<KEY_IN, VAL_IN, OUT> extends BatchSource<KEY_IN, VAL_IN, OUT> {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceBatchSource.class);

  private final ReferencePluginConfig config;

  public ReferenceBatchSource(ReferencePluginConfig config) {
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
   * Record field-level lineage for batch source plugins (ReadOperation). This method should be called from prepareRun
   * of any batch source plugin.
   *
   * @param context BatchSourceContext from prepareRun
   * @param outputName name of output dataset
   * @param tableSchema schema of fields. Also used to determine list of field names. Schema and schema.getFields() must
   * not be null.
   * @param operationName name of the operation
   * @param description operation description; complete sentences preferred
   */
  protected void recordLineage(BatchSourceContext context, String outputName, Schema tableSchema, String operationName,
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
      lineageRecorder.recordRead(operationName, description, fieldNames);
    }
  }
}
