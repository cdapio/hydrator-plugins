/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.format.xls.input;


import com.github.pjfanning.xlsx.StreamingReader;
import com.google.common.base.Preconditions;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.poi.EmptyFileException;
import org.apache.poi.poifs.filesystem.FileMagic;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import javax.annotation.Nullable;


/**
 * {@link XlsInputFormat} is {@link TextInputFormat} implementation for reading Excel files.
 * <p>
 * The {@link XlsInputFormat.XlsRecordReader} reads a given sheet, and within a sheet reads
 * all columns and all rows.
 */
public class XlsInputFormat extends PathTrackingInputFormat {

  public static final String SHEET_NUM = "Sheet Number";
  public static final String SHEET_VALUE = "sheetValue";
  public static final String NAME_SKIP_HEADER = "skipHeader";
  public static final String TERMINATE_IF_EMPTY_ROW = "terminateIfEmptyRow";
  protected static final int EXCEL_BYTE_ARRAY_MAX_OVERRIDE_DEFAULT = Integer.MAX_VALUE / 2;

  @Override
  protected RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(
    FileSplit split, TaskAttemptContext context, @Nullable String pathField,
    @Nullable Schema schema) throws IOException {
    Configuration jobConf = context.getConfiguration();
    boolean skipFirstRow = jobConf.getBoolean(NAME_SKIP_HEADER, false);
    boolean terminateIfEmptyRow = jobConf.getBoolean(TERMINATE_IF_EMPTY_ROW, false);
    Schema outputSchema = schema != null ? Schema.parseJson(context.getConfiguration().get("schema")) : null;
    String sheet = jobConf.get(SHEET_NUM);
    String sheetValue = jobConf.get(SHEET_VALUE, "0");
    return new XlsRecordReader(sheet, sheetValue, outputSchema, terminateIfEmptyRow, skipFirstRow);
  }

  public boolean isSplitable(JobContext context, Path file) {
    return false;
  }

  /**
   * Reads Excel sheet, where each row is a {@link StructuredRecord} and each cell is a field in the record.
   */
  public static class XlsRecordReader extends RecordReader<NullWritable, StructuredRecord.Builder> {
    // Converter for converting xls row to structured record
    XlsRowConverter rowConverter;
    FormulaEvaluator formulaEvaluator;
    // Builder for building structured record
    private StructuredRecord.Builder valueBuilder;
    private FSDataInputStream fileIn;
    // Specifies last row num.
    private int lastRowNum;
    private boolean isRowNull;
    private final String sheet;
    private final String sheetValue;
    private final Schema outputSchema;
    private final boolean terminateIfEmptyRow;
    private final boolean skipFirstRow;
    private int rowCount;
    private Iterator<Row> rows;
    // Specifies the row index.
    private long rowIdx;


    /**
     * Constructor for XlsRecordReader.
     */
    public XlsRecordReader(String sheet, String sheetValue, Schema outputSchema, boolean terminateIfEmptyRow,
                           boolean skipFirstRow) {
      this.sheet = sheet;
      this.sheetValue = sheetValue;
      this.outputSchema = outputSchema;
      this.terminateIfEmptyRow = terminateIfEmptyRow;
      this.skipFirstRow = skipFirstRow;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

      if (!(split instanceof FileSplit)) {
        // should never happen
        throw new IllegalStateException("Input split is not a FileSplit.");
      }
      FileSplit fileSplit = (FileSplit) split;
      Configuration jobConf = context.getConfiguration();
      // Path of input file.
      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(jobConf);
      fileIn = fs.open(file);
      Sheet workSheet;
      Workbook workbook;
      boolean isStreaming = false;
      try {
        // Use Magic Bytes to detect the file type
        InputStream is = FileMagic.prepareToCheckMagic(fileIn);
        byte[] emptyFileCheck = new byte[1];
        is.mark(emptyFileCheck.length);
        if (is.read(emptyFileCheck) < emptyFileCheck.length) {
          throw new EmptyFileException();
        }
        is.reset();

        final FileMagic fm = FileMagic.valueOf(is);
        switch (fm) {
          case OOXML:
            workbook = StreamingReader.builder().rowCacheSize(10).open(is);
            isStreaming = true;
            break;
          case OLE2:
            IOUtils.setByteArrayMaxOverride(EXCEL_BYTE_ARRAY_MAX_OVERRIDE_DEFAULT);
            workbook = WorkbookFactory.create(is);
            formulaEvaluator = workbook.getCreationHelper().createFormulaEvaluator();
            formulaEvaluator.setIgnoreMissingWorkbooks(true);
            break;
          default:
            throw new IOException("Can't open workbook - unsupported file type: " + fm);
        }

        // Check if user wants to access with name or number
        if (sheet.equals(XlsInputFormatConfig.SHEET_NUMBER)) {
          workSheet = workbook.getSheetAt(Integer.parseInt(sheetValue));
        } else {
          workSheet = workbook.getSheet(sheetValue);
        }
        rowConverter = new XlsRowConverter(formulaEvaluator);
      } catch (Exception e) {
        throw new IOException("Exception while reading excel sheet. " + e.getMessage(), e);
      }
      // As we cannot get the number of rows in a sheet while streaming.
      // -1 is used as rowCount to indicate that all rows should be read.
      rowCount = isStreaming ? -1 : workSheet.getPhysicalNumberOfRows();
      lastRowNum = workSheet.getLastRowNum();
      rows = workSheet.iterator();
      isRowNull = false;
      rowIdx = 0;
      valueBuilder = StructuredRecord.builder(outputSchema);
      if (skipFirstRow) {
        Preconditions.checkArgument(rows.hasNext(), "No rows found on sheet %s", sheetValue);
        rowIdx = 1;
        rows.next();
      }
    }

    @Override
    public boolean nextKeyValue() {
      // If any is true, then we stop processing.
      if (!rows.hasNext() || rowCount == 0 || (isRowNull && terminateIfEmptyRow)) {
        return false;
      }
      // Get the next row.
      Row row = rows.next();
      valueBuilder = rowConverter.convert(row, outputSchema);
      if (row == null || valueBuilder == null) {
        isRowNull = true;
        // set valueBuilder to a new builder with all fields set to null
        valueBuilder = StructuredRecord.builder(outputSchema);
      }
      rowIdx++;
      // Stop processing if the row is null and terminateIfEmptyRow is true.
      return !isRowNull || !terminateIfEmptyRow;
    }

    @Override
    public float getProgress() {
      return (float) rowIdx / lastRowNum;
    }

    @Override
    public void close() throws IOException {
      if (fileIn != null) {
        fileIn.close();
      }
    }

    @Override
    public NullWritable getCurrentKey() {
      return NullWritable.get();
    }

    @Override
    public StructuredRecord.Builder getCurrentValue() {
      return valueBuilder;
    }
  }
}
