package co.cask.hydrator.plugin.batch.source;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * {@link ExcelInputFormat} is {@link TextInputFormat} implementation for reading Excel files.
 *
 * The {@link ExcelInputFormat.ExcelRecordReader} reads a given sheet, and within a sheet reads
 * all columns and all rows.
 */
public class ExcelInputFormat extends FileInputFormat {

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
  public static String filePath = "";

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new ExcelRecordReader();
  }

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    // get tokens for all the required FileSystems..
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job.getConfiguration());

    // Whether we need to recursive look into the directory structure
    boolean recursive = getInputDirRecursive(job);

    List<IOException> errors = new ArrayList<IOException>();

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();

    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);

    for (int i = 0; i < dirs.length; ++i) {
      Path p = dirs[i];
      FileSystem fs = p.getFileSystem(job.getConfiguration());
      FileStatus[] matches = fs.globStatus(new Path(filePath));
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
                } else {
                  result.add(stat);
                }
              }
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    return result;
  }

  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  public static void setConfig(String path) {
    filePath = path;
  }


  /**
   * Reads excel spread sheet, where the keys are the offset in the excel file and the text is the complete record.
   */
  public static class ExcelRecordReader extends RecordReader<LongWritable, Text> {

    public static final String END = "END";
    public static final String MID = "MID";

    // Non-printable ASCII character(EOT) to seperate column-values
    public static final String CELL_SEPERATION = String.valueOf((char) 4);

    public static final String COLUMN_SEPERATION = "\r";

    // Map key that represents the row index.
    private LongWritable key;

    // Map value that represents an excel row
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

      Sheet workSheet = null; // sheet can be used as common for XSSF and HSSF workbook
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
        if (rows.hasNext()) {
          rowIdx = 1;
        }
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

      sb.append(row.getRowNum()).append(CELL_SEPERATION);
      sb.append(file).append(CELL_SEPERATION);
      sb.append(row.getSheet().getSheetName()).append(CELL_SEPERATION);

      if (rowCount - 1 == 0 || !rows.hasNext()) {
        sb.append(END).append(CELL_SEPERATION);
      } else {
        sb.append(MID).append(CELL_SEPERATION);
      }
      rowCount--;

      key = new LongWritable(rowIdx);
      while (cellIterator.hasNext()) {
        Cell cell = cellIterator.next();
        String colName = CellReference.convertNumToColString(cell.getColumnIndex());
        switch (cell.getCellType()) {
          case Cell.CELL_TYPE_STRING:
            sb.append(colName)
              .append(COLUMN_SEPERATION).append(cell.getStringCellValue()).append(CELL_SEPERATION);
            break;

          case Cell.CELL_TYPE_BOOLEAN:
            sb.append(colName)
              .append(COLUMN_SEPERATION).append(cell.getBooleanCellValue()).append(CELL_SEPERATION);
            break;

          case Cell.CELL_TYPE_NUMERIC:
            sb.append(colName)
              .append(COLUMN_SEPERATION).append(cell.getNumericCellValue()).append(CELL_SEPERATION);
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
