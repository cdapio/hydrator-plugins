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
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Batch Source to poll fixed length flat files that can be parsed using CobolCopyBook.
 * The plugin will accept the copybook contents in a textbox and a bianry data file.
 * <p>
 * It produces structured record based on the schema as defined either by the copybook contents or as defined by user
 * <p>
 * For the first implementation it will only accept binary fixed length flat files without any nesting
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("CopyBookReader")
@Description("Batch Source to read COBOL copybook fixed length flat files")
public class CopyBookSource extends BatchSource<LongWritable, Map<String, String>, StructuredRecord> {

  @Description("If no file type is mentioned, by default it will be considered as a fixed length flat file")
  public static final int DEFAULT_FILE_STRUCTURE = net.sf.JRecord.Common.Constants.IO_FIXED_LENGTH;

  @Description("Copybook file type supported. For the current implementation the copybook reader will accept only " +
    "fixed flat files")
  public static final List<Integer> SUPPORTED_FILE_STRUCTURES = Lists.newArrayList(
    net.sf.JRecord.Common.Constants.IO_FIXED_LENGTH
  );
  public static final String COPYBOOK_INPUTFORMAT_CBL_CONTENTS = "copybook.inputformat.cbl.contents";
  /* For the initial implementation only fixed length binary files will be accepted.
   This will not handle complex nested structures of COBOL copybook
   Also this will not handle Redefines or iterators in the structure*/
  public static final String COPYBOOK_INPUTFORMAT_FILE_STRUCTURE = "copybook.inputformat.input.filestructure";
  public static final String COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH = "copybook.inputformat.data.hdfs.path";

  private final CopyBookSourceConfig config;

  public CopyBookSource(CopyBookSourceConfig copyBookConfig) {
    this.config = copyBookConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.parseSchema());

  }

  @VisibleForTesting
  final CopyBookSourceConfig getConfig() {
    return config;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    conf.set(COPYBOOK_INPUTFORMAT_CBL_CONTENTS, config.copybookContents);
    if (!(config.fileStructure.isEmpty())) {
      conf.set(COPYBOOK_INPUTFORMAT_FILE_STRUCTURE, config.fileStructure);
    }
    conf.set(COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH, config.binaryFilePath);
    //set the input file path for job
    CopyBookInputFormat.setInputPaths(job, config.binaryFilePath);
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(CopyBookInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<LongWritable, Map<String, String>> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    Map<String, String> value = input.getValue();
    Schema outputSchema;
    if (config.schema == null) {
      outputSchema = getSchema(value.keySet());
    } else {
      outputSchema = config.parseSchema();
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
  private Object parseValue(Schema.Field field, String value) throws IOException {
    Schema.Type fieldType = field.getSchema().getType();
    if (fieldType.equals(Schema.Type.UNION)) {
      fieldType = field.getSchema().getUnionSchema(0).getType();
    }
    switch (fieldType) {
      case NULL:
        return null;
      case INT:
        return Integer.parseInt(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case LONG:
        return Long.parseLong(value);
      case FLOAT:
        return Float.parseFloat(value);
      case BYTES:
        return value.getBytes();
      case STRING:
        return value;
    }
    throw new IOException(String.format("Unsupported schema: %s for field: \'%s\'",
                                        field.getSchema(), field.getName()));
  }

  /**
   * Create schema from list field names retrieved from CopyBookFiles
   *
   * @param filedsSet set of field names extracted from copybook
   * @return output schema fields
   */
  private Schema getSchema(Set<String> filedsSet) {
    //TODO : check int value for data types returned by jrecord to match java data types and create schema accrodindly
  /*found the below solution to map data from copybook to java data types
  Need confirmation to incorporate it.
      *//*data type values and type names references from -
    https://github.com/svn2github/jrecord/blob/00be9a1ff10e57175d3b1ffffdb9b585c55686eb/Source/JRecord_Common/src/net
    /sf/JRecord/External/Def/BasicConversion.java
    https://github.com/svn2github/jrecord/blob/3526120755c77f49985d8ce24dcb980fa79023bb/Source/JRecord_Common/src/net
     /sf/JRecord/Types/Type.java
     Array elements not handled
     *//*
    //date will be of the format yyyy-MM-dd for all cases

    private Object getValues(AbstractFieldValue filedValue) throws ParseException {
      int type = filedValue.getFieldDetail().getType();

      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      SimpleDateFormat simpleDateFormat;
      switch (type) {
      //convert char, char just right , char null terminated, char null paded to string
        case 0:
        case 1:
        case 2:
        case 3:
        case 71:
          return filedValue.asString();
          //extract hex value from filed
        case 4:
          return filedValue.asHex();
          //convert nu, left justified, num right justified , num zero padded to integer
        case 5:
        case 6:
        case 7:
          return filedValue.asInt();
          //extract binary int, binary int positive, positive binary int as integer using the BASE64 encoding
        case 15:
        case 16:
        case 23:
          return Integer.parseInt(Base64.decodeBase64(Base64.encodeBase64(filedValue.toString().getBytes())).
          toString());

        case 17:
          return filedValue.asFloat();
        case 18:
          return filedValue.asDouble();
          //extract decimal, Mainframe Packed Decimal, Mainframe Packed Decimal, Mainframe Zoned Numeric as BigDecimal
        case 8:
        case 11:
        case 19:
        case 20:
        case 22:
        case 24:
        case 25:
        case 26:
        case 27:
        case 28:
        case 29:
        case 31:
        case 32:
        case 33:
          return filedValue.asBigDecimal();
          //extract Binary Integer Big Edian (Mainframe, AIX etc)-  Binary Integer Big Endian (Mainframe?),
          Binary Integer Big Endian (only +ve),Positive Integer Big Endian as BigInt using the BASE64 encoding
        case 35:
        case 36:
        case 39:
          return Base64.decodeInteger(Base64.encodeInteger(filedValue.asBigInteger()));
          //convert date to format "yyyy-MM-dd" and return as string
        case 72:
          simpleDateFormat = new SimpleDateFormat("YYMMDD");
          return dateFormat.format(simpleDateFormat.parse(filedValue.toString()));
        case 73:
          simpleDateFormat = new SimpleDateFormat("YYYYMMDD");
          return dateFormat.format(simpleDateFormat.parse(filedValue.toString()));
        case 74:
          simpleDateFormat = new SimpleDateFormat("DDMMYY");
          return dateFormat.format(simpleDateFormat.parse(filedValue.toString()));
        case 75:
          simpleDateFormat = new SimpleDateFormat("DDMMYYYY");
          return dateFormat.format(simpleDateFormat.parse(filedValue.toString()));
        // return boolean values for checkbox
        case 111:
          if (filedValue.toString().toLowerCase().contains("y")) {
            return true;
          } else {
            return false;
          }
        case 112:
          if (filedValue.toString().toLowerCase().contains("t")) {
            return true;
          } else {
            return false;
          }
        case 114:
          return filedValue.asBoolean();
          //if the tyoe does not match any of the above mentioned types , convert to string and return
        default:
          return filedValue.toString();
      }
    }
      */

    List<Schema.Field> fields = Lists.newArrayList();
    for (String field : filedsSet) {
      fields.add(Schema.Field.of(field, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }
    return Schema.recordOf("record", fields);
  }

  /**
   * Config class for CopyBookSource.
   */
  public static class CopyBookSourceConfig extends ReferencePluginConfig {

    @Name("binaryFilePath")
    @Description("Complete path of the .bin to be read(Eg : hdfs://10.222.41.31:9000/test/DTAR020_FB.bin, " +
      "file:///home/cdap/cdap/DTAR020_FB.bin).\n This will be a fixed length binary format file," +
      " that macthes the copybook." +
      "\n(This is done to accept files present on remote hdfs location)")
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

    @Name("fileStructure")
    @Nullable
    @Description("CopyBook file structure")
    private String fileStructure;

    @Name("copybookContents")
    @Description("Contents of the cobol copybook file which will contain " +
      "the data structure. Eg: \n" +
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

    public CopyBookSourceConfig(String referenceName, String copybookContents, String binaryFilePath, String schema,
                                String fileStructure) {
      super(referenceName);
      this.copybookContents = copybookContents;
      this.binaryFilePath = binaryFilePath;
      this.schema = schema;
      this.fileStructure = fileStructure;
    }

    public CopyBookSourceConfig() {
      super(String.format("CopyBookReader"));
      fileStructure = Integer.toString(DEFAULT_FILE_STRUCTURE);
    }

    @VisibleForTesting
    public final String getFileStructure() {
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
        throw new IllegalArgumentException("Invalid binary file path: " + binaryFilePath);
      } else {
        if (!(binaryFilePath.substring(binaryFilePath.length() - 4).equals(".bin"))) {
          throw new IllegalArgumentException("Invalid binary file path: " + binaryFilePath);
        }
      }
      if (!fileStructure.isEmpty()) {
        try {
          Preconditions.checkArgument(
            SUPPORTED_FILE_STRUCTURES.contains(Integer.parseInt(fileStructure)),
            "Supported file structures: " + SUPPORTED_FILE_STRUCTURES);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("File structure should be integer values. Invalid value for file " +
                                               "structure : " + fileStructure);
        }
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



