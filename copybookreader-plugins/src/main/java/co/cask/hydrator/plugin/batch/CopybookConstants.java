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

import com.google.common.collect.Lists;

import java.util.List;

/**
 *Copybook Contants.
 */
public class CopybookConstants {

  public static final int DEFAULT_FILE_STRUCTURE = net.sf.JRecord.Common.Constants.IO_FIXED_LENGTH;

  //For this initial implementation, only fixed-length binary files will be accepted.
  //Maininting a list to accomodate more file types in the future.
  public static final List<Integer> SUPPORTED_FILE_STRUCTURES = Lists.newArrayList(
    net.sf.JRecord.Common.Constants.IO_FIXED_LENGTH
  );
  public static final String COPYBOOK_INPUTFORMAT_CBL_CONTENTS = "copybook.inputformat.cbl.contents";
  // For this initial implementation, only fixed-length binary files will be accepted.
  // This will not handle complex nested structures of the COBOL copybook, or redefines or iterators in the structure.
  public static final String COPYBOOK_INPUTFORMAT_FILE_STRUCTURE = "copybook.inputformat.input.filestructure";

  public static final String COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH = "copybook.inputformat.data.hdfs.path";

  public static final String MAX_SPLIT_SIZE_DESCRIPTION = "Maximum split-size for each mapper in the MapReduce " +
    "Job. Defaults to 128MB.";

  public static final long DEFAULT_MAX_SPLIT_SIZE = 134217728;
}
