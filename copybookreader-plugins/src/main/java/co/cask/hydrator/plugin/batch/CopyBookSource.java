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
package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import net.sf.JRecord.Common.AbstractFieldValue;
import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Batch source to poll fixed-length flat files that can be parsed using a COBOL copybook.
 * <p>
 * The plugin will accept the copybook contents in a textbox and a binary data file.
 * It produces structured records based on the schema as defined either by the copybook contents or the user.
 * <p>
 * For this first implementation, it will only accept binary fixed-length flat files without any nesting.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("CopybookReader")
@Description("Batch Source to read COBOL Copybook fixed-length flat files")
public class CopybookSource extends BatchSource<LongWritable, Map<String, AbstractFieldValue>, StructuredRecord> {

  private final CopybookSourceConfig config;
  private Schema outputSchema;

  public CopybookSource(CopybookSourceConfig copybookConfig) {
    this.config = copybookConfig;
  }


  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.parseSchema());

  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    if (config.schema != null) {
      outputSchema = config.parseSchema();
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    conf.set(CopybookConstants.COPYBOOK_INPUTFORMAT_CBL_CONTENTS, config.copybookContents);
    conf.setInt(CopybookConstants.COPYBOOK_INPUTFORMAT_FILE_STRUCTURE, config.fileStructure);
    conf.set(CopybookConstants.COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH, config.binaryFilePath);

    // Set the input file path for the job
    CopybookInputFormat.setInputPaths(job, config.binaryFilePath);

    if (config.maxSplitSize != null) {
      conf.setLong(CopybookConstants.MAX_SPLIT_SIZE_DESCRIPTION, config.maxSplitSize);
      CopybookInputFormat.setMaxInputSplitSize(job, conf.getLong(CopybookConstants.MAX_SPLIT_SIZE_DESCRIPTION,
                                                                 CopybookConstants.DEFAULT_MAX_SPLIT_SIZE));
    }
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(CopybookInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<LongWritable, Map<String, AbstractFieldValue>> input,
                        Emitter<StructuredRecord> emitter)
    throws Exception {
    Map<String, AbstractFieldValue> value = input.getValue();
    if (outputSchema == null) {
      outputSchema = getSchema(value.keySet());
    }
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      if (value.containsKey(fieldName)) {
        builder.set(fieldName, parseValue(field, value.get(fieldName)));
      }
    }
    emitter.emit(builder.build());
  }

  /**
   * Convert each field to data type as specified by user in the output schema
   *
   * @param field Schema.Field object defining the field name and properties
   * @param value value of field to be converted to datatype specified by user
   * @return value casted to the required data type
   * @throws IOException
   */
  private Object parseValue(Schema.Field field, AbstractFieldValue value) throws IOException {
    Schema fieldSchema = field.getSchema();
    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema
      .getType();

    switch (fieldType) {
      case NULL:
        return null;
      case INT:
        return value.asInt();
      case DOUBLE:
        return value.asDouble();
      case BOOLEAN:
        return value.asBoolean();
      case LONG:
        return value.asLong();
      case FLOAT:
        return value.asFloat();
      case STRING:
        return value.asString();
    }
    throw new IOException(String.format("Unsupported schema: %s for field: \'%s\'",
                                        field.getSchema(), field.getName()));
  }

  /**
   * Create schema from list field names retrieved from CopybookFiles
   *
   * @param fieldsSet set of field names extracted from Copybook
   * @return output schema fields
   */
  private Schema getSchema(Set<String> fieldsSet) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (String field : fieldsSet) {
      fields.add(Schema.Field.of(field, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }
    return Schema.recordOf("record", fields);
  }

  /**
   * Config class for CopybookSource.
   */
  public static class CopybookSourceConfig extends ReferencePluginConfig {

    @Nullable
    @Description(CopybookConstants.MAX_SPLIT_SIZE_DESCRIPTION)
    public Long maxSplitSize;
    @Name("binaryFilePath")
    @Description("Complete path of the .bin to be read; for example: 'hdfs://10.222.41.31:9000/test/DTAR020_FB.bin' " +
      "or file:///home/cdap/DTAR020_FB.bin'.\n " +
      "This will be a fixed-length binary format file that matches the copybook.\n" +
      "(This is done to accept files present on a remote HDFS location.)")
    private String binaryFilePath;

    @Name("schema")
    @Nullable
    @Description("The schema for the data as it will be formatted in CDAP. Sample schema: " +
      "{\n" +
      "    \"type\": \"record\",\n" +
      "    \"name\": \"schemaBody\",\n" +
      "    \"fields\": [\n" +
      "        {\n" +
      "            \"name\": \"name\",\n" +
      "            \"type\": \"string\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"age\",\n" +
      "            \"type\": \"int\"\n" +
      "        }" +
      "    ]\n" +
      "}")
    private String schema;

    @Nullable
    private Integer fileStructure;

    @Name("copybookContents")
    @Description("Contents of the COBOL copybook file which will contain the data structure. Eg: \n" +
      "000100*                                                                         \n" +
      "000200*   DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML                       \n" +
      "000300*   CENTRAL REPORTING SYSTEM                                              \n" +
      "000400*                                                                         \n" +
      "000500*   CREATED BY BRUCE ARTHUR  19/12/90                                     \n" +
      "000600*                                                                         \n" +
      "000700*   RECORD LENGTH IS 27.                                                  \n" +
      "000800*                                                                         \n" +
      "000900        03  DTAR020-KCODE-STORE-KEY.                                      \n" +
      "001000            05 DTAR020-KEYCODE-NO      PIC X(08).                         \n" +
      "001100            05 DTAR020-STORE-NO        PIC S9(03)   COMP-3.               \n" +
      "001200        03  DTAR020-DATE               PIC S9(07)   COMP-3. ")
    private String copybookContents;

    public CopybookSourceConfig(String referenceName, String copybookContents, String binaryFilePath,
                                @Nullable String schema, @Nullable Integer fileStructure, @Nullable Long maxSplitSize) {
      super(referenceName);
      this.copybookContents = copybookContents;
      this.binaryFilePath = binaryFilePath;
      this.schema = schema;
      this.fileStructure = fileStructure == null ? CopybookConstants.DEFAULT_FILE_STRUCTURE : fileStructure;
      this.maxSplitSize = maxSplitSize == null ? CopybookConstants.DEFAULT_MAX_SPLIT_SIZE : maxSplitSize;
    }

    public CopybookSourceConfig() {
      super(String.format("CopybookReader"));
      fileStructure = CopybookConstants.DEFAULT_FILE_STRUCTURE;
    }

    @VisibleForTesting
    public final int getFileStructure() {
      return fileStructure;
    }
    /**
     * Validate the configuration parameters required
     */
    private void validate() {
      if (schema != null) {
        parseSchema();
      }
      //check the file extension
      if (binaryFilePath.length() < 4) {
        throw new IllegalArgumentException("Invalid binary file path: " + binaryFilePath +
                                             ".The plugin reads binary file as input");
      } else if (!(binaryFilePath.substring(binaryFilePath.length() - 4).equals(".bin"))) {
        throw new IllegalArgumentException("Invalid binary file path: " + binaryFilePath +
                                             ".The plugin reads binary file as input");
      }
      try {
        if (fileStructure != null) {
          Preconditions.checkArgument(
            CopybookConstants.SUPPORTED_FILE_STRUCTURES.contains(fileStructure),
            "Supported file structures: " + CopybookConstants.SUPPORTED_FILE_STRUCTURES);
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("File structure should be integer values. Invalid value for file " +
                                             "structure : " + fileStructure);
      }
    }
    /**
     * Parse the output schema passed by the user
     *
     * @return schema to be used for setting the output
     */
    private Schema parseSchema() {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
      }
    }
  }
}



