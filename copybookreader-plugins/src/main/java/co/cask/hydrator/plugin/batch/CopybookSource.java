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
import co.cask.cdap.api.annotation.Macro;
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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import net.sf.JRecord.Common.AbstractFieldValue;
import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

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

  public static final long DEFAULT_MAX_SPLIT_SIZE_IN_MB = 1;
  private static final long CONVERT_TO_BYTES = 1024 * 1024;

  private final CopybookSourceConfig config;
  private Schema outputSchema;
  private Set<String> fieldsToDrop = new HashSet<String>();

  public CopybookSource(CopybookSourceConfig copybookConfig) {
    this.config = copybookConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    outputSchema = getOutputSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    if (!Strings.isNullOrEmpty(config.drop)) {
      for (String dropField : Splitter.on(",").trimResults().split(config.drop)) {
        fieldsToDrop.add(dropField);
      }
    }
    if (config.maxSplitSize == null) {
      config.maxSplitSize = DEFAULT_MAX_SPLIT_SIZE_IN_MB * CONVERT_TO_BYTES;
    } else {
      config.maxSplitSize = config.maxSplitSize * CONVERT_TO_BYTES;
    }
    outputSchema = getOutputSchema();
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException {
    Job job = JobUtils.createInstance();
    CopybookInputFormat.setCopybookInputformatCblContents(job, config.copybookContents);
    CopybookInputFormat.setBinaryFilePath(job, config.binaryFilePath);
    // Set the input file path for the job
    CopybookInputFormat.setInputPaths(job, config.binaryFilePath);
    CopybookInputFormat.setMaxInputSplitSize(job, config.maxSplitSize);
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(CopybookInputFormat.class,
                                                                                  job.getConfiguration())));
  }

  @Override
  public void transform(KeyValue<LongWritable, Map<String, AbstractFieldValue>> input,
                        Emitter<StructuredRecord> emitter)
    throws Exception {
    Map<String, AbstractFieldValue> value = input.getValue();
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      if (value.containsKey(fieldName)) {
        builder.set(fieldName, getFieldValue(value.get(fieldName)));
      }
    }
    emitter.emit(builder.build());
  }

  /**
   * Get the output schema from the COBOL copybook contents specified by the user.
   *
   * @return outputSchema
   */
  private Schema getOutputSchema() {

    InputStream inputStream = null;
    ExternalRecord externalRecord = null;
    List<Schema.Field> fields = Lists.newArrayList();
    try {
      inputStream = IOUtils.toInputStream(config.copybookContents, "UTF-8");
      BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
      externalRecord = CopybookIOUtils.getExternalRecord(bufferedInputStream);
      String fieldName;
      for (ExternalField field : externalRecord.getRecordFields()) {
        fieldName = field.getName();
        if (fieldsToDrop.contains(fieldName)) {
          continue;
        }
        fields.add(Schema.Field.of(field.getName(), Schema.nullableOf(Schema.of(getFieldSchemaType(field.getType())))));
      }
      return Schema.recordOf("record", fields);
    } catch (IOException e) {
      throw new IllegalArgumentException("Exception while creating input stream for COBOL Copybook. Invalid output " +
                                           "schema: " + e.getMessage(), e);
    } catch (RecordException e) {
      throw new IllegalArgumentException("Exception while creating record from COBOL Copybook. Invalid output " +
                                           "schema: " + e.getMessage(), e);
    }
  }

  /**
   * Get the field Schema.Type from the copybook data types
   *
   * @param type AbstractFiledValue type to be converted to CDAP Schema.Type
   * @return CDAP Schema.Type objects
   */
  private Schema.Type getFieldSchemaType(int type) {
    switch (type) {
      case 0:
        return Schema.Type.STRING;
      case 17:
        return Schema.Type.FLOAT;
      case 18:
      case 22:
      case 31:
      case 32:
      case 33:
        return Schema.Type.DOUBLE;
      case 25:
        return Schema.Type.INT;
      case 35:
      case 36:
      case 39:
        return Schema.Type.LONG;
      default:
        return Schema.Type.STRING;
    }
  }

  /**
   * Get the field values for the fields in the required format.
   * Date will be returned in the format - "yyyy-MM-dd".
   *
   * @param value AbstractFieldValue object to be converted in the JAVA primitive data types
   * @return data objects supported by CDAP
   * @throws ParseException
   */
  private Object getFieldValue(@Nullable AbstractFieldValue value) throws ParseException {
    if (value == null) {
      return null;
    }
    int type = value.getFieldDetail().getType();
    switch (type) {
      case 0:
        return value.asString();
      case 17:
        return value.asFloat();
      case 18:
      case 22:
      case 31:
      case 32:
      case 33:
        return value.asDouble();
      case 25:
        return value.asInt();
      case 35:
      case 36:
      case 39:
        return value.asLong();
      default:
        return value.asString();
    }
  }

  /**
   * Config class for CopybookSource.
   */
  public static class CopybookSourceConfig extends ReferencePluginConfig {

    @Description("Complete path of the .bin to be read; for example: 'hdfs://10.222.41.31:9000/test/DTAR020_FB.bin' " +
      "or 'file:///home/cdap/DTAR020_FB.bin'.\n " +
      "This will be a fixed-length binary format file that matches the copybook.\n" +
      "(This is done to accept files present on a remote HDFS location.)")
    @Macro
    private String binaryFilePath;

    @Description("Contents of the COBOL copybook file which will contain the data structure. For example: \n" +
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

    @Nullable
    @Description("Comma-separated list of fields to drop. For example: 'field1,field2,field3'.")
    private String drop;

    @Nullable
    @Description("Maximum split-size(MB) for each mapper in the MapReduce Job. Defaults to 1MB.")
    @Macro
    private Long maxSplitSize;

    public CopybookSourceConfig() {
      super(String.format("CopybookReader"));
      this.maxSplitSize = DEFAULT_MAX_SPLIT_SIZE_IN_MB * CONVERT_TO_BYTES;
    }

    @VisibleForTesting
    public Long getMaxSplitSize() {
      return maxSplitSize;
    }
  }
}
