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
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroReadSupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * An input format that tracks which the file path each record was read from.
 */
public class PathTrackingInputFormat extends FileInputFormat<NullWritable, StructuredRecord> {
  private static final String PATH_FIELD = "path.tracking.path.field";
  private static final String FILENAME_ONLY = "path.tracking.filename.only";
  private static final String FORMAT = "path.tracking.format";
  private static final String SCHEMA = "path.tracking.inputSchema";
  private static final String AVRO_SCHEMA_KEY = "avro.schema.input.key";
  private static final String PARQUET_SCHEMA_KEY = "parquet.avro.read.schema";
  private static Job job;

  /**
   * Configure the input format to use the specified schema and optional path field.
   */
  public static void configure(Configuration conf, @Nullable String pathField, boolean filenameOnly,
                               String format, String schema) {
    if (pathField != null) {
      conf.set(PATH_FIELD, pathField);
    }
    conf.setBoolean(FILENAME_ONLY, filenameOnly);
    conf.set(FORMAT, format);
    conf.set(SCHEMA, schema);
  }

  public static void setJob(Job currentJob) {
    job = currentJob;
  }

  public static Schema getTextOutputSchema(@Nullable String pathField) {
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
    String format = context.getConfiguration().get(FORMAT);
    String path = filenameOnly ? fileSplit.getPath().getName() : fileSplit.getPath().toUri().toString();
    String schema = context.getConfiguration().get(SCHEMA);
    if ("avro".equalsIgnoreCase(format)) {
      RecordReader<AvroKey<GenericRecord>, NullWritable> delegate = (new AvroKeyInputFormat<GenericRecord>())
        .createRecordReader(split, context);
      return new TrackingAvroRecordReader(delegate, pathField, path, schema, job);
    } else if ("parquet".equalsIgnoreCase(format)) {
      RecordReader<Void, GenericRecord> delegate = (new AvroParquetInputFormat<GenericRecord>())
        .createRecordReader(split, context);
      return new TrackingParquetRecordReader(delegate, pathField, path, schema, job);
    } else {
      RecordReader<LongWritable, Text> delegate = (new TextInputFormat()).createRecordReader(split, context);
      return new TrackingTextRecordReader(delegate, pathField, path);
    }
  }

  private abstract static class TrackingRecordReader<K, V> extends RecordReader<NullWritable, StructuredRecord> {
    protected final RecordReader<K, V> delegate;
    protected final Schema schema;
    protected final String pathField;
    protected final String path;

    protected TrackingRecordReader(RecordReader<K, V> delegate, @Nullable String pathField,
                                   String path, Schema schema) {
      this.delegate = delegate;
      this.pathField = pathField;
      this.path = path;
      this.schema = schema;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.initialize(split, context);
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    protected abstract StructuredRecord getCurrentValue(StructuredRecord.Builder recordBuilder)
      throws IOException, InterruptedException;

    @Override
    public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
      if (pathField != null) {
        recordBuilder.set(pathField, path);
      }
      return getCurrentValue(recordBuilder);
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

  private static class TrackingTextRecordReader extends TrackingRecordReader<LongWritable, Text> {

    private TrackingTextRecordReader(RecordReader<LongWritable, Text> delegate, @Nullable String pathField,
                                     String path) {
      super(delegate, pathField, path, getTextOutputSchema(pathField));
    }

    public StructuredRecord getCurrentValue(StructuredRecord.Builder recordBuilder) throws IOException,
      InterruptedException {
      LongWritable key = delegate.getCurrentKey();
      Text text = delegate.getCurrentValue();

      recordBuilder.set("offset", key.get());
      recordBuilder.set("body", text.toString());
      return recordBuilder.build();
    }
  }

  private static class TrackingAvroRecordReader extends TrackingRecordReader<AvroKey<GenericRecord>, NullWritable> {
    private static AvroToStructuredTransformer recordTransformer;
    private Job job;

    private TrackingAvroRecordReader(RecordReader<AvroKey<GenericRecord>, NullWritable> delegate,
                                     @Nullable String pathField, String path, String schema, Job job)
      throws IOException {
      super(delegate, pathField, path, Schema.parseJson(schema));
      this.job = job;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
      super.initialize(split, context);
      recordTransformer = new AvroToStructuredTransformer();
    }

    public StructuredRecord getCurrentValue(StructuredRecord.Builder recordBuilder) throws IOException,
      InterruptedException {
      AvroJob.setOutputKeySchema(job, new org.apache.avro.Schema.Parser().parse(schema.toString()));
      StructuredRecord ret = recordTransformer.transform(recordBuilder, delegate.getCurrentKey().datum(), schema,
                                                         pathField);
      return ret;
    }
  }

  private static class TrackingParquetRecordReader extends TrackingRecordReader<Void, GenericRecord> {
    private AvroToStructuredTransformer recordTransformer;
    private Job job;

    private TrackingParquetRecordReader(RecordReader<Void, GenericRecord> delegate, @Nullable String pathField,
                                        String path, String schema, Job job) throws IOException {
      super(delegate, pathField, path, Schema.parseJson(schema));
      this.job = job;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      super.initialize(split, context);
      recordTransformer = new AvroToStructuredTransformer();
    }

    public StructuredRecord getCurrentValue(StructuredRecord.Builder recordBuilder) throws IOException,
      InterruptedException {
      StructuredRecord ret = recordTransformer.transform(recordBuilder, delegate.getCurrentValue(), schema, pathField);
      AvroReadSupport.setAvroReadSchema(job.getConfiguration(), new org.apache.avro.Schema.Parser().parse
        (ret.getSchema().toString()));
      return ret;
    }
  }
}
