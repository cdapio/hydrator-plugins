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
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Reads the entire contents of a File into a single record
 */
public class BlobInputFormatter implements FileInputFormatter {
  private final Schema schema;

  BlobInputFormatter(Schema schema) {
    this.schema = schema;
  }

  @Override
  public Map<String, String> getFormatConfig() {
    return Collections.emptyMap();
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord.Builder> create(FileSplit split, TaskAttemptContext context) {
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
