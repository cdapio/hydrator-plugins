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

package co.cask.hydrator.plugin.batch.aggregator;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 * Distinct aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("Distinct")
@Description("Deduplicates input records so that all output records are distinct. " +
  "Can optionally take a list of fields, which will project out all other fields and perform a distinct " +
  "on just those fields.")
public class DistinctAggregator extends RecordAggregator {
  private final Conf conf;
  private Iterable<String> fields;
  private Schema outputSchema;

  /**
   * Plugin Configuration
   */
  public static class Conf extends AggregatorConfig {
    @Nullable
    @Description("Optional comma-separated list of fields to perform the distinct on. If none is given, each record " +
      "will be taken as is. Otherwise, only fields in this list will be considered.")
    private String fields;

    Iterable<String> getFields() {
      return fields == null ? Collections.emptyList() : Splitter.on(',').trimResults().split(fields);
    }
  }

  public DistinctAggregator(Conf conf) {
    super(conf.numPartitions);
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    // if null, the input schema is unknown, or its multiple schemas.
    if (inputSchema == null) {
      stageConfigurer.setOutputSchema(null);
      return;
    }

    // otherwise, we have a constant input schema. Get the output schema and propagate the schema
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, conf.getFields()));
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    outputSchema = context.getOutputSchema();
    fields = conf.getFields();
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) {
    if (fields == null) {
      emitter.emit(record);
      return;
    }

    Schema recordSchema = outputSchema == null ? getOutputSchema(record.getSchema(), fields) : outputSchema;
    StructuredRecord.Builder builder = StructuredRecord.builder(recordSchema);
    for (String fieldName : fields) {
      builder.set(fieldName, record.get(fieldName));
    }
    emitter.emit(builder.build());
  }

  @Override
  public void aggregate(StructuredRecord groupKey, Iterator<StructuredRecord> iterator,
                        Emitter<StructuredRecord> emitter) {
    emitter.emit(groupKey);
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    return getOutputSchema(request.inputSchema, request.getFields());
  }

  private static Schema getOutputSchema(Schema inputSchema, Iterable<String> fields) {
    if (fields == null || !fields.iterator().hasNext()) {
      return inputSchema;
    }

    List<Schema.Field> outputFields = new ArrayList<>();
    for (String fieldName : fields) {
      Schema.Field field = inputSchema.getField(fieldName);
      if (field == null) {
        throw new IllegalArgumentException(String.format("Field %s does not exist in input schema %s.",
                                                         fieldName, inputSchema));
      }
      outputFields.add(field);
    }
    return Schema.recordOf(inputSchema.getRecordName() + ".distinct", outputFields);
  }

  /**
   * Endpoint request for output schema.
   */
  public static class GetSchemaRequest extends Conf {
    private Schema inputSchema;
  }
}
