/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.input;

import co.cask.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Map;

/**
 * Functionality required to read from a Hadoop InputFormat into StructuredRecords
 */
public interface FileInputFormatter {

  /**
   * Get the configuration required by the Hadoop InputFormat. Anything returned in this map will be available
   * through the Hadoop Configuration in {@link #create(FileSplit, TaskAttemptContext)}.
   */
  Map<String, String> getFormatConfig();

  /**
   * Creates a RecordReader that will read the raw data and transform it into a StructuredRecord
   *
   * @param split the input split to read
   * @param context the task context
   * @return a RecordReader for reading and transforming raw input
   */
  RecordReader<NullWritable, StructuredRecord.Builder> create(FileSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException;
}
