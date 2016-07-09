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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 *Reads excel file(s) rows and convert them to structure records.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("ExcelInputReader")
@Description("Batch Source to read one or more excel files and convert rows to structure records using " +
  "input columns, column-mapping and field-type mapping.")
public class ExcelInputReader extends BatchSource<LongWritable, Object, StructuredRecord> {

  public static final String KEY = "key";
  public static final String FILE = "file";
  public static final String SHEET = "sheet";
  public static final String RECORD = "record";
  public static final String EXIT_ON_ERROR = "Exit on error";
  public static final String WRITE_ERROR_DATASET = "Write to error dataset";
  public static final String NULL = "NULL";
  public static final String END = "END";
  public static final String SHEET_NO = "Sheet Number";

  // Non-printable ASCII character(EOT) to seperate column-values
  public static final String CELL_SEPERATION = String.valueOf((char) 4);
  public static final String COLUMN_SEPERATION = "\r";

  private static final Gson GSON = new Gson();
  private static final Type ARRAYLIST_PREPROCESSED_FILES = new TypeToken<ArrayList<String>>() {
  }.getType();
  private final ExcelInputReaderConfig excelInputreaderConfig;
  private Schema outputSchema;
  private Map<String, String> columnMapping = new HashMap<>();
  private Map<String, String> outputSchemaMapping = new HashMap<>();
  private List<String> inputColumns;
  private Map<String, String> outputFieldsMapping = new HashMap<>();
  private BatchRuntimeContext batchRuntimeContext;
  private int prevRowNum;

  public ExcelInputReader(ExcelInputReaderConfig excelReaderConfig) {
    this.excelInputreaderConfig = excelReaderConfig;
  }

  private Schema errorRecordSchema =
    Schema.recordOf("schema",
                    Schema.Field.of(KEY, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(FILE, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(SHEET, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(RECORD, Schema.of(Schema.Type.STRING)));

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    batchRuntimeContext = context;
    init();
  }

  /**
   * Initialize and set maps from input config object
   */
  private void init() {
    if (!Strings.isNullOrEmpty(excelInputreaderConfig.columnList)) {
      String[] columnsList = excelInputreaderConfig.columnList.split(",");
      inputColumns = Arrays.asList(columnsList);
    }

    if (!Strings.isNullOrEmpty(excelInputreaderConfig.columnMapping)) {
      String[] mappings = excelInputreaderConfig.columnMapping.split(",");
      for (String map : mappings) {
        String[] columns = map.split(":");
        if (CollectionUtils.isNotEmpty(inputColumns) && !inputColumns.contains(columns[0])) {
          throw new IllegalArgumentException("Column name: " + columns[0] + " in 'Column-Label Mapping' does not " +
                                               "match the columns in the 'Column To Be Extracted' input text box. " +
                                               "It has to be one of the columns in 'Column To Be Extracted' " +
                                               "input text box.");
        }
        columnMapping.put(columns[0], columns[1]);
      }
    }

    if (!Strings.isNullOrEmpty(excelInputreaderConfig.outputSchema)) {
      String[] schemaList = excelInputreaderConfig.outputSchema.split(",");
      for (String schema : schemaList) {
        String[] columns = schema.split(":");
        if (CollectionUtils.isNotEmpty(inputColumns) && !inputColumns.contains(columns[0])) {
          throw new IllegalArgumentException("Column name: " + columns[0] + " in 'Field Name Schema Type Mapping'" +
                                               " does not match the columns in the 'Column To Be Extracted' input " +
                                               "text box. It has to be one of the columns in " +
                                               "'Column To Be Extracted' input text box.");
        }
        outputSchemaMapping.put(columns[0], columns[1]);
      }
    }
  }

  @Override
  public void transform(KeyValue<LongWritable, Object> input, Emitter<StructuredRecord> emitter) throws Exception {

    getOutputSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    String inputValue = input.getValue().toString();

    String[] excelRecord = inputValue.split(CELL_SEPERATION);

    String fileName = excelRecord[1];
    String sheetName = excelRecord[2];
    String ifEndRow = excelRecord[3];

    int currentRowNum = Integer.parseInt(excelRecord[0]);
    if (currentRowNum - prevRowNum > 1 && excelInputreaderConfig.terminateIfEmptyRow.equalsIgnoreCase("true")) {
      throw new ExecutionException("Encountered empty row while reading Excel file :" + fileName +
                                     " . Terminating processing", new Throwable());
    }
    prevRowNum = currentRowNum;

    Map<String, String> excelColumnValueMap = new HashMap<>();

    for (String column : excelRecord) {
      String[] columnValue = column.split(COLUMN_SEPERATION);
      if (columnValue.length > 1) {
        String name = columnValue[0];
        String value = columnValue[1];

        if (columnMapping.containsKey(name)) {
          excelColumnValueMap.put(columnMapping.get(name), value);
        } else {
          excelColumnValueMap.put(name, value);
        }
      }
    }

    try {
      for (Schema.Field field : outputSchema.getFields()) {
        String fieldName = field.getName();
        if (excelColumnValueMap.containsKey(fieldName)) {
          builder.convertAndSet(fieldName, excelColumnValueMap.get(fieldName));
        } else {
          builder.set(fieldName, NULL);
        }
      }

      builder.set(FILE, new Path(fileName).getName());
      builder.set(SHEET, sheetName);

      emitter.emit(builder.build());

      if (ifEndRow.equalsIgnoreCase(END)) {
        KeyValueTable processedFileMemoryTable = batchRuntimeContext.getDataset(excelInputreaderConfig.memoryTableName);
        processedFileMemoryTable.write(Bytes.toBytes(fileName), Bytes.toBytes(new Date().getTime()));
      }
    } catch (Exception e) {
      switch (excelInputreaderConfig.ifErrorRecord) {
        case EXIT_ON_ERROR:
          throw new IllegalStateException("Terminating processing on error : " + e.getMessage());
        case WRITE_ERROR_DATASET:
          StructuredRecord.Builder errorRecordBuilder = StructuredRecord.builder(errorRecordSchema);
          errorRecordBuilder.set(KEY, fileName + "_" + sheetName + "_" + excelRecord[0]);
          errorRecordBuilder.set(FILE, fileName);
          errorRecordBuilder.set(SHEET, sheetName);
          errorRecordBuilder.set(RECORD, inputValue);
          Table errorTable = batchRuntimeContext.getDataset(excelInputreaderConfig.errorDatasetName);
          errorTable.write(errorRecordBuilder.build());
          break;
        default:
          //ignore on error
          break;
      }
    }
  }

  /**
   * Returns list of all the processed file names which are kept in memory table.
   * @param batchSourceContext
   * @return processedFiles
   */
  private List<String> getAllProcessedFiles(BatchSourceContext batchSourceContext) {
    List<String> processedFiles = new ArrayList<>();
    if (!excelInputreaderConfig.reprocess) {
      KeyValueTable table = batchSourceContext.getDataset(excelInputreaderConfig.memoryTableName);
      processedFiles = new ArrayList<>();
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.DATE, -Integer.valueOf(excelInputreaderConfig.tableExpiryPeriod));
      Date expiryDate = cal.getTime();
      try (CloseableIterator<KeyValue<byte[], byte[]>> filesIterable = table.scan(null, null)) {
        while (filesIterable.hasNext()) {
          KeyValue<byte[], byte[]> file = filesIterable.next();
          Long time = Bytes.toLong(file.getValue());
          Date processedDate = new Date(time);
          if (processedDate.before(expiryDate)) {
            table.delete(file.getKey());
          } else {
            processedFiles.add(new String(file.getKey(), Charsets.UTF_8));
          }
        }
      }
    }
    return processedFiles;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    if (excelInputreaderConfig.sheet.equalsIgnoreCase(SHEET_NO) &&
      !StringUtils.isNumeric(excelInputreaderConfig.sheetValue)) {
      throw new IllegalArgumentException("Invalid sheet number. The value should be greater than or equals to zero.");
    }

    if (Strings.isNullOrEmpty(excelInputreaderConfig.columnList) &&
      Strings.isNullOrEmpty(excelInputreaderConfig.outputSchema)) {
      throw new IllegalArgumentException("'Field Name Schema Type Mapping' input cannot be empty when the empty " +
                                           "input value of 'Columns To Be Extracted' is provided.");
    }

    try {
      if (!Strings.isNullOrEmpty(excelInputreaderConfig.errorDatasetName)) {
        Map<String, String> properties = new HashMap<>();
        properties.put(Properties.Table.PROPERTY_SCHEMA, errorRecordSchema.toString());
        properties.put(Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, KEY);
        DatasetProperties datasetProperties = DatasetProperties.builder().addAll(properties).build();
        pipelineConfigurer.createDataset(excelInputreaderConfig.errorDatasetName, Table.class, datasetProperties);

      } else if (excelInputreaderConfig.ifErrorRecord.equalsIgnoreCase(WRITE_ERROR_DATASET)) {
        throw new IllegalArgumentException("Error dataset name should not be empty while choosing write to error " +
                                             "dataset for 'On Error' input.");
      }
      pipelineConfigurer.createDataset(excelInputreaderConfig.memoryTableName, KeyValueTable.class);
    } catch (Exception e) {
      throw new IllegalStateException("Exception while creating dataset.", e);
    }

    init();
    getOutputSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) throws Exception {

    Job job = JobUtils.createInstance();

    String processFiles = GSON.toJson(getAllProcessedFiles(batchSourceContext), ARRAYLIST_PREPROCESSED_FILES);

    ExcelInputFormat.setConfigurations(job, excelInputreaderConfig.filePattern, excelInputreaderConfig.sheet,
                                       excelInputreaderConfig.reprocess, excelInputreaderConfig.sheetValue,
                                       excelInputreaderConfig.columnList, excelInputreaderConfig.skipFirstRow,
                                       excelInputreaderConfig.terminateIfEmptyRow, excelInputreaderConfig.rowsLimit,
                                       excelInputreaderConfig.ifErrorRecord, processFiles);

    // Sets the input path(s).
    ExcelInputFormat.addInputPaths(job, excelInputreaderConfig.filePath);

    // Sets the filter based on extended class implementation.
    ExcelInputFormat.setInputPathFilter(job, ExcelReaderRegexFilter.class);
    SourceInputFormatProvider inputFormatProvider = new SourceInputFormatProvider(ExcelInputFormat.class,
                                                                                  job.getConfiguration());
    batchSourceContext.setInput(Input.of(excelInputreaderConfig.referenceName, inputFormatProvider));

  }

  /**
   * Get the output schema from the Excel Input Reader specified by the user.
   * @return outputSchema
   */
  private void getOutputSchema() {
    if (outputSchema == null) {
      List<Schema.Field> outputFields = Lists.newArrayList();
      outputFields.add(Schema.Field.of(FILE, Schema.of(Schema.Type.STRING)));
      outputFields.add(Schema.Field.of(SHEET, Schema.of(Schema.Type.STRING)));
      try {
        // If processing of all the columns are required
        if (inputColumns == null || inputColumns.isEmpty()) {
          for (String fieldName : outputSchemaMapping.keySet()) {
            String columnName = fieldName;
            if (columnMapping.containsKey(fieldName)) {
              columnName = columnMapping.get(fieldName);
            }

            Schema fieldType = Schema.of(Schema.Type.valueOf(outputSchemaMapping.get(fieldName).toUpperCase()));
            outputFields.add(Schema.Field.of(columnName, fieldType));
          }
        } else {
          for (String column : inputColumns) {
            String columnName = column;
            if (columnMapping.containsKey(column)) {
              columnName = columnMapping.get(column);
            }
            if (outputSchemaMapping.containsKey(column)) {
              Schema fieldType = Schema.of(Schema.Type.valueOf(outputSchemaMapping.get(column).toUpperCase()));
              outputFields.add(Schema.Field.of(columnName, fieldType));
            } else {
              outputFields.add(Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
            }

            outputFieldsMapping.put(column, columnName);
          }
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Exception while creating output schema for Excel input reader. " +
                                             "Invalid output " + "schema: " + e.getMessage(), e);
      }
      outputSchema = Schema.recordOf("outputSchema", outputFields);
    }
  }


  /**
   * Config class for ExcelInputReader.
   */
  public static class ExcelInputReaderConfig extends ReferencePluginConfig {

    @Name("filePath")
    @Description("Path of the excel file(s) to be read; for example: 'file:///home/cdap' for a " +
      "local path and 'hdfs://<namemode-hostname>:9000/cdap' for a path in hdfs.")
    private String filePath;

    @Name("filePattern")
    @Description("Regex pattern to select specific file(s); for example: '.*'")
    private String filePattern;

    @Name("memoryTableName")
    @Description("KeyValue table name to keep the track of processed files. This can be a new table or existing one;" +
      " for example: 'inventory-memory-table'")
    private String memoryTableName;

    @Description("Expiry period (days) for data in the table. Default is 30 days." +
      "Example - For tableExpiryPeriod = 30, data before 30 days get deleted from the table.")
    private String tableExpiryPeriod;

    @Name("reprocess")
    @Description("Specifies whether the file(s) should be reprocessed. " +
      "Options to select are true or false")
    private boolean reprocess;

    @Name("sheet")
    @Description("Specifies whether sheet has to be processed by sheet name or sheet no; " +
      "Shift 'Options are' in next line: " +
      "Sheet Name" +
      "Sheet Number")
    private String sheet;

    @Name("sheetValue")
    @Description("Specifies the value corresponding to 'sheet' input. Can be either sheet name or sheet no; " +
      "for example: 'Sheet1' or '1' in case user selects 'Sheet Name' or 'Sheet Number' as 'sheet' input respectively.")
    private String sheetValue;

    @Nullable
    @Name("columnList")
    @Description("Specify the excel column names which needs to be extracted from the excel sheet; for example: 'A,B'.")
    private String columnList;

    @Nullable
    @Name("columnMapping")
    @Description("List of the excel column name to be renamed. The key specifies the name of the column to rename, " +
      "with its corresponding value specifying the new name for that column names; for example A:id,B:name")
    private String columnMapping;

    @Name("skipFirstRow")
    @Description("Specify whether first row in the excel sheet is to be skipped or not. " +
      "Options to select are true or false.")
    private boolean skipFirstRow;

    @Name("terminateIfEmptyRow")
    @Description("Specify whether processing needs to be terminated in case an empty row is encountered " +
      "while processing excel files. Options to select are true or false.")
    private String terminateIfEmptyRow;

    @Nullable
    @Name("rowsLimit")
    @Description("Specify maximum number of rows to be processed for each sheet; for example: '100'.")
    private String rowsLimit;

    @Nullable
    @Name("outputSchema")
    @Description("Comma separated mapping of column names in the output schema to the data types;" +
      "for example: 'A:string,B:int'. This input is required if no inputs " +
      "for 'columnList' has been provided.")
    private String outputSchema;

    @Name("ifErrorRecord")
    @Description("Specifies the action to be taken in case if an error occurs. Shift 'Options are' in next line: " +
      "Ignore error and continue" +
      "Exit on error: Stops processing upon encountering an error" +
      "Write to error dataset:  Writes the error record to an error dataset and continues processing.")
    private String ifErrorRecord;

    @Nullable
    @Name("errorDatasetName")
    @Description("Name of the table to store error record; for example: 'error-table-name'.")
    private String errorDatasetName;

    public ExcelInputReaderConfig() {
      super(String.format("ExcelInputReader"));
    }
  }
}
