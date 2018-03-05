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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;

import java.io.IOException;
import java.util.Iterator;


/**
 * {@link ExcelInputFormat} is {@link TextInputFormat} implementation for reading Excel files.
 *
 * The {@link ExcelInputFormat.ExcelRecordReader} reads a given sheet, and within a sheet reads
 * all columns and all rows.
 */
public class ExcelInputFormat extends TextInputFormat {

  public static final String SHEET_NAME = "Sheet Name";
  public static final String RE_PROCESS = "reprocess";
  public static final String COLUMN_LIST = "columnList";
  public static final String SKIP_FIRST_ROW = "skipFirstRow";
  public static final String TERMINATE_IF_EMPTY_ROW = "terminateIfEmptyRow";
  public static final String ROWS_LIMIT = "rowsLimit";
  public static final String IF_ERROR_RECORD = "IfErrorRecord";
  public static final String PROCESSED_FILES = "processedFiles";
  public static final String FILE_PATTERN = "filePattern";
  public static final String SHEET = "sheet";
  public static final String SHEET_VALUE = "sheetValue";

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new ExcelRecordReader();
  }

  public static void setConfigurations(Job job, String filePattern, String sheet, boolean reprocess,
                                       String sheetValue, String columnList, boolean skipFirstRow,
                                       String terminateIfEmptyRow, String rowLimit, String ifErrorRecord,
                                       String processedFiles) {

    Configuration configuration = job.getConfiguration();
    configuration.set(FILE_PATTERN, filePattern);
    configuration.set(SHEET, sheet);
    configuration.setBoolean(RE_PROCESS, reprocess);
    configuration.set(SHEET_VALUE, sheetValue);
    configuration.setBoolean(SKIP_FIRST_ROW, skipFirstRow);
    configuration.set(TERMINATE_IF_EMPTY_ROW, terminateIfEmptyRow);

    if (!Strings.isNullOrEmpty(rowLimit)) {
      configuration.set(ROWS_LIMIT, rowLimit);
    }

    if (!Strings.isNullOrEmpty(columnList)) {
      configuration.set(COLUMN_LIST, columnList);
    }

    configuration.set(IF_ERROR_RECORD, ifErrorRecord);
    configuration.set(PROCESSED_FILES, processedFiles);
  }


  /**
   * Reads excel spread sheet, where the keys are the offset in the excel file and the text is the complete record.
   */
  public static class ExcelRecordReader extends RecordReader<LongWritable, Text> {

    public static final String END = "END";
    public static final String MID = "MID";

    // Non-printable ASCII character(EOT) to seperate column-values
    public static final String CELL_SEPERATOR = String.valueOf((char) 4);

    public static final String COLUMN_SEPERATOR = "\r";

    // Map key that represents the row index.
    private LongWritable key;

    // Map value that represents an excel row
    private Text value;

    // Specifies all the rows of an Excel spreadsheet - An iterator over all the rows.
    private Iterator<Row> rows;

    // InputStream handler for Excel files.
    private FSDataInputStream fileIn;

    // Path of input file.
    private Path file;

    // Specifies the row index.
    private long rowIdx;

    // Specifies last row num.
    private int lastRowNum;

    //Keeps row limits
    private int rowCount;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException,
      InterruptedException {

      FileSplit split = (FileSplit) genericSplit;
      Configuration job = context.getConfiguration();
      file = split.getPath();

      FileSystem fs = file.getFileSystem(job);
      fileIn = fs.open(split.getPath());

      // Reads the excel file, selects the sheet to be read.
      String sheet = job.get(SHEET);
      String sheetValue = job.get(SHEET_VALUE);

      Sheet workSheet; // sheet can be used as common for XSSF and HSSF workbook
      try {
        Workbook workbook = WorkbookFactory.create(fileIn);
        if (sheet.equalsIgnoreCase(SHEET_NAME)) {
          workSheet = workbook.getSheet(sheetValue);
        } else {
          workSheet = workbook.getSheetAt(Integer.parseInt(sheetValue));
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Exception while reading excel sheet. " + e.getMessage(), e);
      }

      rowCount = job.getInt(ROWS_LIMIT, workSheet.getPhysicalNumberOfRows());
      rows = workSheet.iterator();
      lastRowNum = workSheet.getLastRowNum();
      rowIdx = 0;

      boolean skipFirstRow = job.getBoolean(SKIP_FIRST_ROW, false);
      if (skipFirstRow) {
        Preconditions.checkArgument(rows.hasNext(), "No rows found on sheet %s", sheetValue);
        rowIdx = 1;
        rows.next();
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!rows.hasNext() || rowCount == 0) {
        return false;
      }

      StringBuilder sb = new StringBuilder();

      // Get the next row.
      Row row = rows.next();

      // For each row, iterate through each columns
      Iterator<Cell> cellIterator = row.cellIterator();

      sb.append(row.getRowNum()).append(CELL_SEPERATOR);
      sb.append(file).append(CELL_SEPERATOR);
      sb.append(row.getSheet().getSheetName()).append(CELL_SEPERATOR);

      if (rowCount - 1 == 0 || !rows.hasNext()) {
        sb.append(END).append(CELL_SEPERATOR);
      } else {
        sb.append(MID).append(CELL_SEPERATOR);
      }
      rowCount--;

      key = new LongWritable(rowIdx);
      while (cellIterator.hasNext()) {
        Cell cell = cellIterator.next();
        String colName = CellReference.convertNumToColString(cell.getColumnIndex());
        switch (cell.getCellType()) {
          case Cell.CELL_TYPE_STRING:
            sb.append(colName)
              .append(COLUMN_SEPERATOR).append(cell.getStringCellValue()).append(CELL_SEPERATOR);
            break;

          case Cell.CELL_TYPE_BOOLEAN:
            sb.append(colName)
              .append(COLUMN_SEPERATOR).append(cell.getBooleanCellValue()).append(CELL_SEPERATOR);
            break;

          case Cell.CELL_TYPE_NUMERIC:
            if (HSSFDateUtil.isCellDateFormatted(cell)) {
              sb.append(colName).append(COLUMN_SEPERATOR).append(cell.getDateCellValue()).append(CELL_SEPERATOR);
            } else {
              sb.append(colName)
                  .append(COLUMN_SEPERATOR).append(cell.getNumericCellValue()).append(CELL_SEPERATOR);
            }
            break;
        }
      }
      value = new Text(sb.toString());
      rowIdx++;

      return true;
    }

    @Override
    public float getProgress() throws IOException {
        return rowIdx / lastRowNum;
    }

    @Override
    public void close() throws IOException {
      if (fileIn != null) {
        fileIn.close();
      }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }
  }
}
