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
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.InputFile;
import io.cdap.cdap.etl.api.validation.InputFiles;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.format.input.PathTrackingConfig;
import io.cdap.plugin.format.input.PathTrackingInputFormatProvider;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.poi.EmptyFileException;
import org.apache.poi.poifs.filesystem.FileMagic;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Reads XLS(X) into StructuredRecords.
 */
@Plugin(type = ValidatingInputFormat.PLUGIN_TYPE)
@Name(XlsInputFormatProvider.NAME)
@Description(XlsInputFormatProvider.DESC)
public class XlsInputFormatProvider extends PathTrackingInputFormatProvider<XlsInputFormatConfig> {
  static final String NAME = "xls";
  static final String DESC = "Plugin for reading files in xls(x) format.";
  public static final PluginClass PLUGIN_CLASS = PluginClass.builder()
    .setType(ValidatingInputFormat.PLUGIN_TYPE)
    .setName(NAME)
    .setDescription(DESC)
    .setClassName(XlsInputFormatProvider.class.getName())
    .setConfigFieldName("conf")
    .setProperties(XlsInputFormatConfig.XLS_FIELDS)
    .build();
  private final XlsInputFormatConfig conf;

  public XlsInputFormatProvider(XlsInputFormatConfig conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public String getInputFormatClassName() {
    return XlsInputFormat.class.getName();
  }

  @Override
  public void validate(FormatContext context) {
    Schema schema = super.getSchema(context);
    FailureCollector collector = context.getFailureCollector();
    // When the sheet is specified by number, the sheet value must be a number
    if (!conf.containsMacro(XlsInputFormatConfig.NAME_SHEET_VALUE) &&
      conf.getSheet().equals(XlsInputFormatConfig.SHEET_NUMBER) &&
      !Strings.isNullOrEmpty(conf.getSheetValue())) {
      getSheetAsNumber(collector);
    }
    if (!conf.containsMacro(PathTrackingConfig.NAME_SCHEMA) && schema == null && context.getInputSchema() == null) {
      collector.addFailure("XLS format cannot be used without specifying a schema.", "Schema must be specified.")
        .withConfigProperty(PathTrackingConfig.NAME_SCHEMA);
    }
  }

  @Override
  protected void addFormatProperties(Map<String, String> properties) {
    properties.put(XlsInputFormat.SHEET_NUM, conf.getSheet());
    if (!Strings.isNullOrEmpty(conf.getSheetValue())) {
      properties.put(XlsInputFormat.SHEET_VALUE, conf.getSheetValue());
    }
    properties.put(XlsInputFormat.NAME_SKIP_HEADER, String.valueOf(conf.getSkipHeader()));
    properties.put(XlsInputFormat.TERMINATE_IF_EMPTY_ROW, String.valueOf(conf.getTerminateIfEmptyRow()));
    properties.put(FileInputFormat.SPLIT_MINSIZE, Long.toString(Long.MAX_VALUE));
  }

  @Override
  @Nullable
  public Schema detectSchema(FormatContext context, InputFiles inputFiles) throws IOException {
    String blankHeader = "BLANK";
    FailureCollector failureCollector = context.getFailureCollector();
    DataFormatter formatter = new DataFormatter();
    for (InputFile inputFile : inputFiles) {
      try (Workbook workbook = getWorkbook(inputFile.open())) {
        Sheet workSheet;
        // Check if user wants to access with name or number
        if (conf.getSheet() != null && conf.getSheet().equals(XlsInputFormatConfig.SHEET_NUMBER)) {
          Integer sheetValue = getSheetAsNumber(failureCollector);
          if (sheetValue == null) {
            return null;
          }
          workSheet = workbook.getSheetAt(sheetValue);
        } else {
          if (Strings.isNullOrEmpty(conf.getSheetValue())) {
            failureCollector.addFailure("Sheet name must be specified.", null)
              .withConfigProperty(XlsInputFormatConfig.NAME_SHEET_VALUE);
            return null;
          }
          workSheet = workbook.getSheet(conf.getSheetValue());
        }

        // If provided sheet does not exist, throw an exception
        if (workSheet == null) {
          failureCollector.addFailure("Sheet " + conf.getSheetValue() + " does not exist in the workbook.",
                                      "Specify a valid sheet.");
          return null;
        }

        int sampleSize = conf.getSampleSize();
        // Row numbers are 0 based in POI
        int rowStart = Math.min(0, workSheet.getFirstRowNum());
        int rowEnd = Math.min(sampleSize, workSheet.getLastRowNum());

        int lastCellNumMax = 0;
        List<String> columnNames = new ArrayList<>();
        XlsInputFormatSchemaDetector schemaDetector = new XlsInputFormatSchemaDetector();
        Iterator<Row> rows = workSheet.iterator();
        for (int rowIndex = rowStart; rowIndex <= rowEnd && rows.hasNext(); rowIndex++) {
          Row row = rows.next();
          if (row == null) {
            continue;
          }
          lastCellNumMax = Math.max(lastCellNumMax, row.getLastCellNum());

          // Use the first row to get the column names
          if (rowIndex == 0 && conf.getSkipHeader()) {
            for (int cellIndex = 0; cellIndex < lastCellNumMax; cellIndex++) {
              Cell cell = row.getCell(cellIndex, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
              columnNames.add(cell == null ? blankHeader : formatter.formatCellValue(cell));
            }
            // Skip Header
            continue;
          }

          for (int cellIndex = 0; cellIndex < lastCellNumMax; cellIndex++) {
            Cell cell = row.getCell(cellIndex, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
            boolean isFirstRow = rowIndex == (conf.getSkipHeader() ? 1 : 0);
            schemaDetector.reduceSchema(cellIndex, cell, isFirstRow);
          }

        }

        // If some rows have more cells than the first row, add blank headers for the extra cells
        if (lastCellNumMax > columnNames.size() && conf.getSkipHeader()) {
          for (int i = columnNames.size(); i < lastCellNumMax; i++) {
            columnNames.add(blankHeader);
          }
        }

        // Set column names if header is not skipped
        if (!conf.getSkipHeader()) {
          for (int i = 0; i < lastCellNumMax; i++) {
            columnNames.add(CellReference.convertNumToColString(i));
          }
        }

        Schema schema = Schema.recordOf("xls", schemaDetector.getFields(
          XlsInputFormatUtils.getSafeColumnNames(columnNames)));
        return PathTrackingInputFormatProvider.addPathField(context.getFailureCollector(), schema, conf.getPathField());
      }
    }
    return null;
  }

  private Integer getSheetAsNumber(FailureCollector failureCollector) {
    if (!Strings.isNullOrEmpty(conf.getSheetValue())) {
      try {
        int sheetValue = Integer.parseInt(conf.getSheetValue());
        if (sheetValue >= 0) {
          return sheetValue;
        }
        failureCollector.addFailure("Sheet number must be a positive number.", null)
          .withConfigProperty(XlsInputFormatConfig.NAME_SHEET_VALUE);
      } catch (NumberFormatException e) {
        failureCollector.addFailure("Sheet number must be a number.", null)
          .withConfigProperty(XlsInputFormatConfig.NAME_SHEET_VALUE);
      }
    }
    return 0;
  }

  private Workbook getWorkbook(InputStream fileIn) {
    Workbook workbook;
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
          break;
        case OLE2:
          // workaround for large files
          IOUtils.setByteArrayMaxOverride(XlsInputFormat.EXCEL_BYTE_ARRAY_MAX_OVERRIDE_DEFAULT);
          workbook = WorkbookFactory.create(is);
          break;
        default:
          throw new IOException("Can't open workbook - unsupported file type: " + fm);
      }
      return workbook;
    } catch (Exception e) {
      throw new IllegalArgumentException("Exception while reading excel sheet. " + e.getMessage(), e);
    }

  }

}
