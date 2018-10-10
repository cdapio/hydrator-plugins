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

package co.cask.hydrator.format.input.parquet;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.hydrator.format.input.PathTrackingInputFormat;
import co.cask.hydrator.format.input.avro.PathTrackingAvroInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * A wrapper class that's responsible for delegating to a corresponding RecordReader in
 * {@link PathTrackingInputFormat}. All it does is pick the i'th path in the CombineFileSplit to create a
 * FileSplit and use the delegate RecordReader to read that split.
 */
public class CombineParquetRecordReader extends CombineFileRecordReaderWrapper<NullWritable, StructuredRecord> {

  public CombineParquetRecordReader(CombineFileSplit split, TaskAttemptContext context,
                                    Integer idx) throws IOException, InterruptedException {
    super(new PathTrackingParquetInputFormat(), split, context, idx);
  }
}
