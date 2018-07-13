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
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.io.InvalidRecordException;

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
  private static final String SCHEMA = "path.tracking.schema";

  /**
   * Configure the input format to use the specified schema and optional path field.
   */
  public static void configure(Job job, Configuration conf, @Nullable String pathField, boolean filenameOnly,
                               String format, @Nullable String schema) {
    if (pathField != null) {
      conf.set(PATH_FIELD, pathField);
    }
    conf.setBoolean(FILENAME_ONLY, filenameOnly);
    conf.set(FORMAT, format);
    if (schema != null) {
      conf.set(SCHEMA, schema);
      if (format.equalsIgnoreCase("avro")) {
        AvroJob.setInputKeySchema(job, new org.apache.avro.Schema.Parser().parse(schema));
      } else if (format.equalsIgnoreCase("parquet")) {
        AvroWriteSupport.setSchema(conf, new org.apache.avro.Schema.Parser().parse(schema));
        // TODO: (CDAP-13140) remove
        conf.set("parquet.avro.read.schema", schema);
      }
    } else if (format.equalsIgnoreCase("text")) {
      conf.set(SCHEMA, getTextOutputSchema(pathField).toString());
    }
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

  public static Schema addFieldToSchema(Schema schema, String addFieldKey) {
    List<Schema.Field> newFields = new ArrayList<>(schema.getFields().size() + 1);
    newFields.addAll(schema.getFields());
    Schema.Field newField = Schema.Field.of(addFieldKey, Schema.of(Schema.Type.STRING));
    newFields.add(newField);
    return Schema.recordOf(schema.getRecordName(), newFields);
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
    Schema parsedSchema = schema == null ? null : Schema.parseJson(schema);
    if ("avro".equalsIgnoreCase(format)) {
      RecordReader<AvroKey<GenericRecord>, NullWritable> delegate = (new AvroKeyInputFormat<GenericRecord>())
        .createRecordReader(split, context);
      return new TrackingAvroRecordReader(delegate, pathField, path, parsedSchema);
    } else if ("parquet".equalsIgnoreCase(format)) {
      RecordReader<Void, GenericRecord> delegate = (new AvroParquetInputFormat<GenericRecord>())
        .createRecordReader(split, context);
      return new TrackingParquetRecordReader(delegate, pathField, path, parsedSchema);
    } else {
      RecordReader<LongWritable, Text> delegate = (new TextInputFormat()).createRecordReader(split, context);
      return new TrackingTextRecordReader(delegate, pathField, path, parsedSchema);
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

    protected abstract StructuredRecord.Builder startCurrentValue() throws IOException, InterruptedException;

    @Override
    public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
      StructuredRecord.Builder recordBuilder = startCurrentValue();
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

  private static class TrackingTextRecordReader extends TrackingRecordReader<LongWritable, Text> {

    private TrackingTextRecordReader(RecordReader<LongWritable, Text> delegate, @Nullable String pathField,
                                     String path, Schema schema) {
      super(delegate, pathField, path, schema);
    }

    public StructuredRecord.Builder startCurrentValue() throws IOException, InterruptedException {
      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
      LongWritable key = delegate.getCurrentKey();
      Text text = delegate.getCurrentValue();

      recordBuilder.set("offset", key.get());
      recordBuilder.set("body", text.toString());
      return recordBuilder;
    }
  }

  private static class TrackingAvroRecordReader extends TrackingRecordReader<AvroKey<GenericRecord>, NullWritable> {
    private static AvroToStructuredTransformer recordTransformer;

    private TrackingAvroRecordReader(RecordReader<AvroKey<GenericRecord>, NullWritable> delegate,
                                     @Nullable String pathField, String path, Schema schema)
      throws IOException {
      super(delegate, pathField, path, schema);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
      super.initialize(split, context);
      recordTransformer = new AvroToStructuredTransformer();
    }

    public StructuredRecord.Builder startCurrentValue() throws IOException, InterruptedException {
      GenericRecord genericRecord = delegate.getCurrentKey().datum();
      Schema recordSchema = schema;
      if (recordSchema == null) {
        recordSchema = recordTransformer.convertSchema(genericRecord.getSchema());
        if (pathField != null) {
          recordSchema = addFieldToSchema(recordSchema, pathField);
        }
      }
      return recordTransformer.transform(genericRecord, recordSchema, pathField);
    }
  }

  private static class TrackingParquetRecordReader extends TrackingRecordReader<Void, GenericRecord> {
    private AvroToStructuredTransformer recordTransformer;

    private TrackingParquetRecordReader(RecordReader<Void, GenericRecord> delegate, @Nullable String pathField,
                                        String path, Schema schema) throws IOException {
      super(delegate, pathField, path, schema);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      try {
        super.initialize(split, context);
      } catch (InvalidRecordException e) {
        // TODO: (CDAP-13140) remove
        throw new RuntimeException("There is a mismatch between read and write schema. " +
                                     "Please modify the plugin schema to include all fields in the write schema.", e);
      }
      recordTransformer = new AvroToStructuredTransformer();
    }

    public StructuredRecord.Builder startCurrentValue() throws IOException, InterruptedException {
      GenericRecord genericRecord = delegate.getCurrentValue();
      Schema recordSchema = schema;
      if (recordSchema == null) {
        recordSchema = recordTransformer.convertSchema(genericRecord.getSchema());
        if (pathField != null) {
          recordSchema = addFieldToSchema(recordSchema, pathField);
        }
      }
      return recordTransformer.transform(genericRecord, recordSchema, pathField);
    }
  }
}
