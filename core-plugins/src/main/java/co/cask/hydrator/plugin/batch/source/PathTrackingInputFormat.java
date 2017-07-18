/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.hydrator.plugin.common.AvroToStructuredTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
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
import parquet.avro.AvroParquetInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * An input format that tracks which the file path each record was read from.
 */
public class PathTrackingInputFormat extends FileInputFormat<NullWritable, StructuredRecord> {
  private static final String PATH_FIELD = "path.tracking.path.field";
  private static final String FILENAME_ONLY = "path.tracking.filename.only";

  private static String fileSourceFormat = "text";

  /**
   * Configure the input format to use the specified schema and optional path field.
   */
  public static void configure(Configuration conf, @Nullable String pathField, boolean filenameOnly,
                               String format) {
    if (pathField != null) {
      conf.set(PATH_FIELD, pathField);
    }
    conf.setBoolean(FILENAME_ONLY, filenameOnly);
    if (format != null) {
      fileSourceFormat = format;
    }
  }

  public static Schema getOutputSchema(@Nullable String pathField) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of("offset", Schema.of(Schema.Type.LONG)));
    fields.add(Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    if (pathField != null) {
      fields.add(Schema.Field.of(pathField, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }
    return Schema.recordOf("file.record", fields);
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit split,
                                                                         TaskAttemptContext context)
    throws IOException, InterruptedException {

    if (!(split instanceof FileSplit)) {
      // should never happen
      throw new IllegalStateException("Input split is not a FileSplit.");
    }
    FileSplit fileSplit = (FileSplit) split;
    String pathField = context.getConfiguration().get(PATH_FIELD);
    boolean filenameOnly = context.getConfiguration().getBoolean(FILENAME_ONLY, false);
    String path = filenameOnly ? fileSplit.getPath().getName() : fileSplit.getPath().toUri().toString();
    fileSourceFormat = fileSourceFormat.toLowerCase();
    if (Objects.equals(fileSourceFormat, "avro")) {
      RecordReader<AvroKey<GenericRecord>, NullWritable> delegate = (new AvroKeyInputFormat<GenericRecord>())
        .createRecordReader(split, context);
      return new TrackingAvroRecordReader(delegate, pathField, path);
    } else if (Objects.equals(fileSourceFormat, "parquet")) {
      RecordReader<Void, GenericRecord> delegate = (new AvroParquetInputFormat<GenericRecord>())
        .createRecordReader(split, context);
      return new TrackingParquetRecordReader(delegate, pathField, path);
    } else {
      RecordReader<LongWritable, Text> delegate = (new TextInputFormat()).createRecordReader(split, context);
      return new TrackingTextRecordReader(delegate, pathField, path);
    }
  }

  private static class TrackingTextRecordReader extends RecordReader<NullWritable, StructuredRecord> {
    private final RecordReader<LongWritable, Text> delegate;
    private final Schema schema;
    private final String pathField;
    private final String path;

    private TrackingTextRecordReader(RecordReader<LongWritable, Text> delegate, @Nullable String pathField,
                                     String path) {
      this.delegate = delegate;
      this.pathField = pathField;
      this.path = path;
      schema = getOutputSchema(pathField);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.initialize(split, context);
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
      LongWritable key = delegate.getCurrentKey();
      Text text = delegate.getCurrentValue();

      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema)
        .set("offset", key.get())
        .set("body", text.toString());
      if (pathField != null) {
        recordBuilder.set(pathField, path);
      }
      return recordBuilder.build();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return delegate.nextKeyValue();
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

  private static class TrackingAvroRecordReader extends RecordReader<NullWritable, StructuredRecord> {
    private final RecordReader<AvroKey<GenericRecord>, NullWritable> delegate;
    private final Schema schema;
    private final String pathField;
    private final String path;

    private TrackingAvroRecordReader(RecordReader<AvroKey<GenericRecord>, NullWritable> delegate,
                                     @Nullable String pathField, String path) {
      this.delegate = delegate;
      this.pathField = pathField;
      this.path = path;
      schema = getOutputSchema(pathField);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.initialize(split, context);
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
      AvroToStructuredTransformer recordTransformer = new AvroToStructuredTransformer();
      return recordTransformer.transform(delegate.getCurrentKey().datum());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return delegate.nextKeyValue();
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

  private static class TrackingParquetRecordReader extends RecordReader<NullWritable, StructuredRecord> {
    private final RecordReader<Void, GenericRecord> delegate;
    private final Schema schema;
    private final String pathField;
    private final String path;

    private TrackingParquetRecordReader(RecordReader<Void, GenericRecord> delegate,
                                     @Nullable String pathField, String path) {
      this.delegate = delegate;
      this.pathField = pathField;
      this.path = path;
      schema = getOutputSchema(pathField);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.initialize(split, context);
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
      AvroToStructuredTransformer recordTransformer = new AvroToStructuredTransformer();
      return recordTransformer.transform(delegate.getCurrentValue());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return delegate.nextKeyValue();
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
