/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.format.input;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * An input format that tracks which the file path each record was read from. This InputFormat is a wrapper around
 * underlying input formats. The responsibility of this class is to keep track of which file each record is reading
 * from, and to add the file URI to each record. In addition, for text files, it can be configured to keep track
 * of the header for the file, which underlying record readers can use.
 */
public abstract class PathTrackingInputFormat extends FileInputFormat<NullWritable, StructuredRecord> {
  /**
   * This property is used to configure the record readers to emit the header as the first record read,
   * regardless of if it is actually in the input split.
   * This is a hack to support wrangler's parse-as-csv with header and will be removed once
   * there is a proper solution in wrangler.
   */
  public static final String COPY_HEADER = "path.tracking.copy.header";
  static final String PATH_FIELD = "path.tracking.path.field";
  static final String FILENAME_ONLY = "path.tracking.filename.only";
  public static final String SOURCE_FILE_ENCODING = "path.tracking.encoding";
  static final String SCHEMA = "schema";
  public static final String TARGET_ENCODING = "utf-8";

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit split,
                                                                         TaskAttemptContext context)
    throws IOException, InterruptedException {

    if (!(split instanceof FileSplit)) {
      // should never happen
      throw new IllegalStateException("Input split is not a FileSplit.");
    }
    FileSplit fileSplit = (FileSplit) split;
    Configuration hConf = context.getConfiguration();
    String pathField = hConf.get(PATH_FIELD);
    boolean userFilenameOnly = hConf.getBoolean(FILENAME_ONLY, false);
    String path = userFilenameOnly ? fileSplit.getPath().getName() : fileSplit.getPath().toUri().toString();
    String schema = hConf.get(SCHEMA);
    Schema parsedSchema = schema == null ? null : Schema.parseJson(schema);

    RecordReader<NullWritable, StructuredRecord.Builder> delegate = createRecordReader(fileSplit, context,
                                                                                       pathField, parsedSchema);
    return new TrackingRecordReader(delegate, pathField, path);
  }

  public RecordReader<LongWritable, Text> getDefaultRecordReaderDelegate(InputSplit split,
                                                                            TaskAttemptContext context) {
    RecordReader<LongWritable, Text> delegate;

    if (context.getConfiguration().get(SOURCE_FILE_ENCODING) != null) {
      String encoding = context.getConfiguration().get(SOURCE_FILE_ENCODING);
      delegate = (new CharsetTransformingPathTrackingInputFormat(encoding)).createRecordReader(split, context);
    } else {
      delegate = (new TextInputFormat()).createRecordReader(split, context);
    }

    return delegate;
  }

  protected abstract RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(
    FileSplit split, TaskAttemptContext context,
    @Nullable String pathField, @Nullable Schema schema) throws IOException, InterruptedException;

  /**
   * Supports adding a field to each record that contains the path of the file the record was read from.
   */
  static class TrackingRecordReader extends RecordReader<NullWritable, StructuredRecord> {
    private final RecordReader<NullWritable, StructuredRecord.Builder> delegate;
    private final String pathField;
    private final String path;

    TrackingRecordReader(RecordReader<NullWritable, StructuredRecord.Builder> delegate,
                         @Nullable String pathField, String path) {
      this.delegate = delegate;
      this.pathField = pathField;
      this.path = path;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.initialize(split, context);
    }

    @Override
    public NullWritable getCurrentKey() {
      return NullWritable.get();
    }

    @Override
    public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
      StructuredRecord.Builder recordBuilder = delegate.getCurrentValue();
      if (pathField != null) {
        recordBuilder.set(pathField, path);
      }
      return recordBuilder.build();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return delegate.getProgress();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return delegate.nextKeyValue();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
