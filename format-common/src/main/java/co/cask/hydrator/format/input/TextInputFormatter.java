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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Input reading logic for text files.
 */
public class TextInputFormatter implements FileInputFormatter {
  private final Schema schema;

  TextInputFormatter(Schema schema) {
    this.schema = schema;
  }

  @Override
  public Map<String, String> getFormatConfig() {
    return Collections.emptyMap();
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord.Builder> create(FileSplit split, TaskAttemptContext context) {
    RecordReader<LongWritable, Text> delegate = (new TextInputFormat()).createRecordReader(split, context);
    String header = context.getConfiguration().get(CombinePathTrackingInputFormat.HEADER);
    return new TextRecordReader(delegate, schema, header);
  }

  /**
   * Text record reader
   */
  static class TextRecordReader extends RecordReader<NullWritable, StructuredRecord.Builder> {
    private final RecordReader<LongWritable, Text> delegate;
    private final Schema schema;
    private final String header;
    private boolean emittedHeader;

    TextRecordReader(RecordReader<LongWritable, Text> delegate, Schema schema, @Nullable String header) {
      this.delegate = delegate;
      this.schema = schema;
      this.header = header;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.initialize(split, context);
      emittedHeader = false;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (header != null && !emittedHeader) {
        return true;
      }

      if (delegate.nextKeyValue()) {
        // if this record is the actual header and we've already emitted the copied header,
        // skip this record so that the header is not emitted twice.
        if (emittedHeader && delegate.getCurrentKey().get() == 0L) {
          return delegate.nextKeyValue();
        }
        return true;
      }
      return false;
    }

    @Override
    public NullWritable getCurrentKey() {
      return NullWritable.get();
    }

    @Override
    public StructuredRecord.Builder getCurrentValue() throws IOException, InterruptedException {
      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
      if (header != null && !emittedHeader) {
        emittedHeader = true;
        recordBuilder.set("offset", 0L);
        recordBuilder.set("body", header);
      } else {
        recordBuilder.set("offset", delegate.getCurrentKey().get());
        recordBuilder.set("body", delegate.getCurrentValue().toString());
      }
      return recordBuilder;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return delegate.getProgress();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
