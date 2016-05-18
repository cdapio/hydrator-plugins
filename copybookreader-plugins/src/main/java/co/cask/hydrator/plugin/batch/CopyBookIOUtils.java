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

import net.sf.JRecord.Common.CommonBits;
import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.Details.LineProvider;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.External.ToLayoutDetail;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.LineIOProvider;
import net.sf.JRecord.Numeric.Convert;
import net.sf.cb2xml.def.Cb2xmlConstants;

import java.io.IOException;
import java.io.InputStream;

/**
 * Utility class to parse and read COBOL Copybook and binary data file contents.
 */
public class CopybookIOUtils {

  private static final String FONT = "cp037";

  /**
   * Get the schema properties from the Copybook contents
   *
   * @param cblIs Input stream for COBOL Copybook contents
   * @return ExternalRecord object defining the schema fields and their properties
   * @throws RecordException
   */
  public static ExternalRecord getExternalRecord(InputStream cblIs) throws RecordException {
    CommonBits.setDefaultCobolTextFormat(Cb2xmlConstants.USE_STANDARD_COLUMNS);
    CobolCopybookLoader copybookInt = new CobolCopybookLoader();
    ExternalRecord record = copybookInt.loadCopyBook(cblIs, "", CopybookLoader.SPLIT_NONE, 0, FONT,
                                                     Convert.FMT_MAINFRAME, 0, null);
    return record;
  }

  /**
   * Get record length for each line
   *
   * @param externalRecord ExternalRecord object defining the schema fields and their properties
   * @param fileStructure  File structure of the data file
   * @return the record length of each line
   */
  public static int getRecordLength(ExternalRecord externalRecord, int fileStructure) {
    int recordByteLength = 0;
    for (ExternalField field : externalRecord.getRecordFields()) {
      recordByteLength += field.getLen();
    }
    if (Constants.IO_VB == fileStructure) {
      recordByteLength += 4;
    }
    return recordByteLength;
  }

  /**
   * Get the AbstractLine reader object to read each field value from the binary file
   *
   * @param is             Input stream containing binary data file contents
   * @param fileStructure  File structure of the data file
   * @param externalRecord ExternalRecord object defining the schema fields and their properties
   * @return
   * @throws RecordException
   * @throws IOException
   */
  public static AbstractLineReader getAndOpenLineReader(InputStream is, int fileStructure,
                                                        ExternalRecord externalRecord)
    throws RecordException, IOException {

    LineProvider lineProvider = LineIOProvider.getInstance().getLineProvider(fileStructure, FONT);
    AbstractLineReader reader = LineIOProvider.getInstance().getLineReader(fileStructure, lineProvider);
    LayoutDetail copybook = ToLayoutDetail.getInstance().getLayout(externalRecord);
    reader.open(is, copybook);
    return reader;
  }
}
