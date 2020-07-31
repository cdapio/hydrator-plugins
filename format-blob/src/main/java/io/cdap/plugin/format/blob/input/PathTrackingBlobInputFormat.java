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

package io.cdap.plugin.format.blob.input;

import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Blob input format
 */
public class PathTrackingBlobInputFormat extends PathTrackingInputFormat {

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    return JobUtils.applyWithExtraClassLoader(job, getClass().getClassLoader(),
                                              PathTrackingBlobInputFormat.super::getSplits);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    // Blobs should not be splitable.
    return false;
  }

  @Override
  protected RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(FileSplit split,
                                                                                    TaskAttemptContext context,
                                                                                    @Nullable String pathField,
                                                                                    @Nullable Schema schema) {
    if (split.getLength() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Blob format cannot be used with files larger than 2GB");
    }
    return new RecordReader<NullWritable, StructuredRecord.Builder>() {
      boolean hasNext;
      byte[] val;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) {
        hasNext = true;
        val = null;
      }

      @Override
      public boolean nextKeyValue() throws IOException {
        if (!hasNext) {
          return false;
        }
        hasNext = false;
        if (split.getLength() == 0) {
          return false;
        }

        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        try (FSDataInputStream input = fs.open(path)) {
          val = new byte[(int) split.getLength()];
          ByteStreams.readFully(input, val);
        }
        return true;
      }

      @Override
      public NullWritable getCurrentKey() {
        return NullWritable.get();
      }

      @Override
      public StructuredRecord.Builder getCurrentValue() {
        String fieldName = schema.getFields().iterator().next().getName();
        return StructuredRecord.builder(schema).set(fieldName, val);
      }

      @Override
      public float getProgress() {
        return 0.0f;
      }

      @Override
      public void close() {
        // no-op
      }
    };

  }
}
