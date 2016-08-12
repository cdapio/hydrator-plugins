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

package co.cask.hydrator.plugin.batch.joiner;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.JoinConfig;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.MultiInputPipelineConfigurer;
import co.cask.cdap.etl.api.MultiInputStageConfigurer;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchJoinerContext;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Path;

/**
 * Batch joiner to join records from multiple inputs
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name("Joiner")
@Description("Performs join operation on records from each input based on required inputs. If all the inputs are " +
  "required inputs, inner join will be performed. Otherwise inner join will be performed on required inputs and " +
  "records from non-required inputs will only be present if they match join criteria. If there are no required " +
  "inputs, outer join will be performed")
public class Joiner extends BatchJoiner<StructuredRecord, StructuredRecord, StructuredRecord> {
  private final JoinerConfig conf;
  private Map<String, Schema> inputSchemas;
  private Schema outputSchema;
  private Map<String, List<String>> perStageJoinKeys;
  private Table<String, String, String> perStageSelectedFields;
  private Set<String> requiredInputs;
  private Multimap<String, String> duplicateFields = ArrayListMultimap.create();

  public Joiner(JoinerConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(MultiInputPipelineConfigurer pipelineConfigurer) {
    MultiInputStageConfigurer stageConfigurer = pipelineConfigurer.getMultiInputStageConfigurer();
    Map<String, Schema> inputSchemas = stageConfigurer.getInputSchemas();
    init(inputSchemas);
    //validate the input schema and get the output schema for it
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchemas));
  }

  @Override
  public void prepareRun(BatchJoinerContext context) throws Exception {
    if (conf.getNumPartitions() != null) {
      context.setNumPartitions(conf.getNumPartitions());
    }
  }

  @Override
  public void initialize(BatchJoinerRuntimeContext context) throws Exception {
    init(context.getInputSchemas());
    inputSchemas = context.getInputSchemas();
    outputSchema = context.getOutputSchema();
  }

  @Override
  public StructuredRecord joinOn(String stageName, StructuredRecord record) throws Exception {
    List<Schema.Field> fields = new ArrayList<>();
    Schema schema = record.getSchema();

    List<String> joinKeys = perStageJoinKeys.get(stageName);
    int i = 1;
    for (String joinKey : joinKeys) {
      Schema.Field joinField = Schema.Field.of(String.valueOf(i++), schema.getField(joinKey).getSchema());
      fields.add(joinField);
    }
    Schema keySchema = Schema.recordOf("join.key", fields);
    StructuredRecord.Builder keyRecordBuilder = StructuredRecord.builder(keySchema);
    i = 1;
    for (String joinKey : joinKeys) {
      keyRecordBuilder.set(String.valueOf(i++), record.get(joinKey));
    }

    return keyRecordBuilder.build();
  }

  @Override
  public JoinConfig getJoinConfig() {
    return new JoinConfig(requiredInputs);
  }

  @Override
  public StructuredRecord merge(StructuredRecord joinKey, Iterable<JoinElement<StructuredRecord>> joinRow) {
    StructuredRecord.Builder outRecordBuilder = StructuredRecord.builder(outputSchema);

    for (JoinElement<StructuredRecord> joinElement : joinRow) {
      String stageName = joinElement.getStageName();
      StructuredRecord record = joinElement.getInputRecord();

      Map<String, String> selectedFields = perStageSelectedFields.row(stageName);

      for (Schema.Field field : record.getSchema().getFields()) {
        String inputFieldName = field.getName();

        // drop the field if not part of fieldsToRename
        if (!selectedFields.containsKey(inputFieldName)) {
          continue;
        }

        String outputFieldName = selectedFields.get(inputFieldName);
        outRecordBuilder.set(outputFieldName, record.get(inputFieldName));
      }
    }
    return outRecordBuilder.build();
  }

  void init(Map<String, Schema> inputSchemas) {
    validateJoinKeySchemas(inputSchemas);
    requiredInputs = conf.getInputs();
    perStageSelectedFields = conf.getPerStageSelectedFields();
  }

  void validateJoinKeySchemas(Map<String, Schema> inputSchemas) {
    perStageJoinKeys = conf.getPerStageJoinKeys();

    if (perStageJoinKeys.size() != inputSchemas.size()) {
      throw new IllegalArgumentException("There should be join keys present from each stage");
    }

    List<Schema> prevSchemaList = null;
    for (Map.Entry<String, List<String>> entry : perStageJoinKeys.entrySet()) {
      ArrayList<Schema> schemaList = new ArrayList<>();
      String stageName = entry.getKey();

      Schema schema = inputSchemas.get(stageName);
      if (schema == null) {
        throw new IllegalArgumentException(String.format("Input schema for input stage %s can not be null", stageName));
      }

      for (String joinKey : entry.getValue()) {
        Schema.Field field = schema.getField(joinKey);
        schemaList.add(field.getSchema());
      }
      if (prevSchemaList != null && !prevSchemaList.equals(schemaList)) {
        throw new IllegalArgumentException(String.format("For stage %s, Schemas of joinKeys %s are expected to be: " +
                                                           "%s, but found: %s",
                                                         stageName, entry.getValue(), prevSchemaList.toString(),
                                                         schemaList.toString()));
      }
      prevSchemaList = schemaList;
    }
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    try {
      validateJoinKeySchemas(request.inputSchemas);
      requiredInputs = request.getInputs();
      perStageSelectedFields = request.getPerStageSelectedFields();
      duplicateFields = ArrayListMultimap.create();
      return getOutputSchema(request.inputSchemas);
    } catch (IllegalArgumentException e) {
        throw new BadRequestException(e.getMessage());
    }
  }

  /**
   * Endpoint request for output schema.
   */
  public static class GetSchemaRequest extends JoinerConfig {
    public Map<String, Schema> inputSchemas;
  }

  Schema getOutputSchema(Map<String, Schema> inputSchemas) {
    validateRequiredInputs(inputSchemas);

    // stage name to input schema
    Map<String, Schema> inputs = new HashMap<>(inputSchemas);
    // Selected Field name to output field info
    Map<String, OutputFieldInfo> outputFieldInfo = new LinkedHashMap<>();
    List<String> duplicateAliases = new ArrayList<>();

    // order of fields in output schema will be same as order of selectedFields
    Set<Table.Cell<String, String, String>> rows = perStageSelectedFields.cellSet();
    for (Table.Cell<String, String, String> row : rows) {
      String stageName = row.getRowKey();
      String inputFieldName = row.getColumnKey();
      String alias = row.getValue();
      Schema inputSchema = inputs.get(stageName);

      if (inputSchema == null) {
        throw new IllegalArgumentException(String.format("Input schema for input stage %s can not be null", stageName));
      }

      if (outputFieldInfo.containsKey(alias)) {
        OutputFieldInfo outInfo = outputFieldInfo.get(alias);
        if (duplicateAliases.add(alias)) {
          duplicateFields.put(outInfo.getStageName(), outInfo.getInputFieldName());
        }
        duplicateFields.put(stageName, inputFieldName);
        continue;
      }

      Schema.Field inputField = inputSchema.getField(inputFieldName);
      if (inputField == null) {
        throw new IllegalArgumentException(String.format(
          "Invalid field: %s of stage '%s' does not exist in input schema %s.",
          inputFieldName, stageName, inputSchema));
      }
      // set nullable fields for non-required inputs
      if (requiredInputs.contains(stageName)) {
        outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
                                                       Schema.Field.of(alias, inputField.getSchema())));
      } else {
        outputFieldInfo.put(alias, new OutputFieldInfo(alias, stageName, inputFieldName,
                                                       Schema.Field.of(alias,
                                                                       Schema.nullableOf(inputField.getSchema()))));
      }
    }

    if (!duplicateFields.isEmpty()) {
      throw new IllegalArgumentException(String.format("Output schema must not have any duplicate field names, but " +
                                                         "found duplicate fields: %s for aliases: %s", duplicateFields,
                                                       duplicateAliases));
    }
    return Schema.recordOf("join.output", getOutputFields(outputFieldInfo.values()));
  }

  private List<Schema.Field> getOutputFields(Collection<OutputFieldInfo> fieldsInfo) {
    List<Schema.Field> outputFields = new ArrayList<>();
    for (OutputFieldInfo fieldInfo : fieldsInfo) {
      outputFields.add(fieldInfo.getField());
    }
    return outputFields;
  }

  /**
   * Class to hold information about output fields
   */
  private class OutputFieldInfo {
    private String name;
    private String stageName;
    private String inputFieldName;
    private Schema.Field field;

    OutputFieldInfo(String name, String stageName, String inputFieldName, Schema.Field field) {
      this.name = name;
      this.stageName = stageName;
      this.inputFieldName = inputFieldName;
      this.field = field;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getStageName() {
      return stageName;
    }

    public void setStageName(String stageName) {
      this.stageName = stageName;
    }

    public String getInputFieldName() {
      return inputFieldName;
    }

    public void setInputFieldName(String inputFieldName) {
      this.inputFieldName = inputFieldName;
    }

    public Schema.Field getField() {
      return field;
    }

    public void setField(Schema.Field field) {
      this.field = field;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      OutputFieldInfo that = (OutputFieldInfo) o;

      if (!name.equals(that.name)) {
        return false;
      }
      if (!stageName.equals(that.stageName)) {
        return false;
      }
      if (!inputFieldName.equals(that.inputFieldName)) {
        return false;
      }
      return field.equals(that.field);
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + stageName.hashCode();
      result = 31 * result + inputFieldName.hashCode();
      result = 31 * result + field.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "OutputFieldInfo{" +
        "name='" + name + '\'' +
        ", stageName='" + stageName + '\'' +
        ", inputFieldName='" + inputFieldName + '\'' +
        ", field=" + field +
        '}';
    }
  }

  private void validateRequiredInputs(Map<String, Schema> inputSchemas) {
    for (String requiredInput : requiredInputs) {
      if (!inputSchemas.containsKey(requiredInput)) {
        throw new IllegalArgumentException(String.format("Provided required input %s is not an input stage name.",
                                                         requiredInput));
      }
    }
  }
}
