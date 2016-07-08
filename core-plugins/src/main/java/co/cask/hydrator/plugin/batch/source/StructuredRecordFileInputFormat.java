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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.List;

/**
 * Wrapper around a FileInputFormat that attempts to capture the path each record was read from.
 */
public class StructuredRecordFileInputFormat extends FileInputFormat<NullWritable, StructuredRecord> {
  public static final Schema SCHEMA = Schema.recordOf(
    "fileRecord",
    Schema.Field.of("offset", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("path", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
  );
  public static final String INPUT_FORMAT_CLASS = "hydrator.file.input.format.class";

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    FileInputFormat<?, ?> inputFormat = getInputFormat(job.getConfiguration());
    return inputFormat.getSplits(job);
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new StructuredRecordReader();
  }

  private static FileInputFormat<?, ?>  getInputFormat(Configuration conf) throws IOException {
    String className = conf.get(INPUT_FORMAT_CLASS);
    try {
      Class<?> inputFormatClass = conf.getClassByName(className);
      Object inputFormatObj = inputFormatClass.newInstance();
      if (!(inputFormatObj instanceof FileInputFormat)) {
        throw new IllegalArgumentException(String.format("%s is not a FileInputFormat", className));
      }
      return (FileInputFormat<?, ?>) inputFormatObj;
    } catch (ClassNotFoundException e) {
      throw new IOException(String.format("Input format class %s could not be found.", className), e);
    } catch (InstantiationException e) {
      throw new IOException(String.format("Input format class %s could not be instantiated.", className), e);
    } catch (IllegalAccessException e) {
      throw new IOException(String.format("Illegal access while instantiating input format class %s.", className), e);
    }
  }

  /**
   * Record reader that tries to get the path each key-value was read from by looking at the input split.
   */
  public static class StructuredRecordReader extends RecordReader<NullWritable, StructuredRecord> {
    private String path;
    private RecordReader delegate;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      if (split instanceof FileSplit) {
        path = ((FileSplit) split).getPath().toUri().toString();
      } else if (split instanceof CombineFileSplit) {
        path = Joiner.on(',').join(((CombineFileSplit) split).getPaths());
      } else {
        path = null;
      }
      Configuration conf = context.getConfiguration();
      String className = conf.get(INPUT_FORMAT_CLASS);
      try {
        Class<?> inputFormatClass = conf.getClassByName(className);
        Object inputFormatObj = inputFormatClass.newInstance();
        if (!(inputFormatObj instanceof InputFormat)) {
          throw new IOException(String.format("%s is not an input format", className));
        }
        InputFormat inputFormat = (InputFormat) inputFormatObj;
        delegate = inputFormat.createRecordReader(split, context);
        delegate.initialize(split, context);
      } catch (ClassNotFoundException e) {
        throw new IOException(String.format("Input format class %s could not be found.", className), e);
      } catch (InstantiationException e) {
        throw new IOException(String.format("Input format class %s could not be instantiated.", className), e);
      } catch (IllegalAccessException e) {
        throw new IOException(String.format("Illegal access while instantiating input format class %s.", className), e);
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return delegate.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
      Object key = delegate.getCurrentKey();
      Object val = delegate.getCurrentValue();
      return StructuredRecord.builder(SCHEMA)
        .set("path", path)
        .set("offset", key instanceof LongWritable ? ((LongWritable) key).get() : null)
        .set("body", val == null ? null : val.toString())
        .build();
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
