/*
 * Copyright © 2018-2020 Cask Data, Inc.
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

package io.cdap.plugin.format.parquet.input;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.List;

/**
 * Combined input format that tracks which file each parquet record was read from.
 */
public class CombineParquetInputFormat extends CombineFileInputFormat<NullWritable, StructuredRecord> {

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    return JobUtils.applyWithExtraClassLoader(job, getClass().getClassLoader(),
                                              CombineParquetInputFormat.super::getSplits);
  }

  /**
   * Creates a RecordReader that delegates to some other RecordReader for each path in the input split.
   */
  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    return new CombineFileRecordReader<>((CombineFileSplit) split, context, WrapperReader.class);
  }

  /**
   * A wrapper class that's responsible for delegating to a corresponding RecordReader in
   * {@link PathTrackingInputFormat}. All it does is pick the i'th path in the CombineFileSplit to create a
   * FileSplit and use the delegate RecordReader to read that split.
   */
  public static class WrapperReader extends CombineFileRecordReaderWrapper<NullWritable, StructuredRecord> {

    public WrapperReader(CombineFileSplit split, TaskAttemptContext context,
                         Integer idx) throws IOException, InterruptedException {
      super(new PathTrackingParquetInputFormat(), split, context, idx);
    }
  }
}
