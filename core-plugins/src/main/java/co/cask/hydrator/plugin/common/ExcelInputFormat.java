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

package co.cask.hydrator.plugin.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;


/**
 * {@link ExcelInputFormat} is {@link TextInputFormat} implementation for reading Excel files.
 *
 * The {@link ExcelInputFormat.ExcelRecordReader} reads a given sheet, and within a sheet reads
 * all columns and all rows.
 */
public class ExcelInputFormat extends TextInputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(ExcelInputFormat.class);

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new ExcelRecordReader();
  }

  /**
   * Reads excel spread sheet, where the keys are the offset in the excel file and the text is the complete record.
   */
  public static class ExcelRecordReader extends RecordReader<LongWritable, Text> {
    // Map key that represents the row index.
    private LongWritable key;

    // Map value that represents : // {file}\r{sheet}\r{type}\t{col}\t{value}\r{type}\r{col}\r{value}...
    private Text value;

    // Represents a indvidual cell
    private Cell cell;

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

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException,
      InterruptedException {

      FileSplit split = (FileSplit) genericSplit;
      Configuration job = context.getConfiguration();
      file = split.getPath();

      FileSystem fs = file.getFileSystem(job);
      fileIn = fs.open(split.getPath());

      // Reads the excel file, selects the sheet to be read.
      HSSFWorkbook workbook = new HSSFWorkbook(fileIn);

      String sheetName = job.get("excel.sheet.name");
      int sheetIndx = job.getInt("excel.sheet.index", -1); // Index overrides name.

      HSSFSheet sheet;
      if (sheetIndx < 0 && sheetName != null && !sheetName.isEmpty()) {
        sheet = workbook.getSheet(sheetName);
      } else {
        sheet = workbook.getSheetAt(sheetIndx);
      }

      rows = sheet.rowIterator();
      lastRowNum = sheet.getLastRowNum();
      rowIdx = 0;

      boolean skipFirstRow = job.getBoolean("excel.sheet.skip.first.row", false);
      if (skipFirstRow && rows.hasNext()) {
        rows.next(); // Reads the first row.
        rowIdx = 1;
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if(! rows.hasNext()) {
        return false;
      }

      StringBuilder sb = new StringBuilder();

      // Get the next row.
      Row row = rows.next();

      // For each row, iterate through each columns
      Iterator<Cell> cellIterator = row.cellIterator();

      sb.append(file.getName()).append("\r");
      sb.append(row.getSheet()).append("\r");
      key = new LongWritable(rowIdx);
      int col = 0;
      while(cellIterator.hasNext()) {
        Cell cell = cellIterator.next();
        int q = col % 26;
        String cellname = (q == 0) ? String.valueOf('A' + col) :
          new String(String.valueOf('A' + q) + String.valueOf('A' + col));
        switch (cell.getCellType()) {
          case Cell.CELL_TYPE_STRING:
            sb.append("s").append("\r").append(cellname)
              .append("\r").append(cell.getStringCellValue()).append("\r");
            break;

          case Cell.CELL_TYPE_BOOLEAN:
            sb.append("b").append("\r").append(cellname)
              .append("\r").append(cell.getStringCellValue()).append("\r");
            break;

          case Cell.CELL_TYPE_NUMERIC:
            sb.append("n").append("\r").append(cellname)
              .append("\r").append(cell.getStringCellValue()).append("\r");
            break;
        }
        col++;
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
