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

package co.cask.hydrator.format;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.format.plugin.FileSourceProperties;
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
 * An input format that tracks which the file path each record was read from. This InputFormat is a wrapper around
 * underlying input formats. The responsibility of this class is to keep track of which file each record is reading
 * from, and to add the file URI to each record. In addition, for text files, it can be configured to keep track
 * of the header for the file, which underlying record readers can use.
 *
 * This is tightly coupled with {@link CombinePathTrackingInputFormat}.
 * TODO: (CDAP-14406) clean up File input formats.
 */
public class PathTrackingInputFormat extends FileInputFormat<NullWritable, StructuredRecord> {
  /**
   * This property is used to configure the record readers to emit the header as the first record read,
   * regardless of if it is actually in the input split.
   * This is a hack to support wrangler's parse-as-csv with header and will be removed once
   * there is a proper solution in wrangler.
   */
  public static final String COPY_HEADER = "path.tracking.copy.header";
  private static final String PATH_FIELD = "path.tracking.path.field";
  private static final String FILENAME_ONLY = "path.tracking.filename.only";
  private static final String FORMAT = "path.tracking.format";
  private static final String SCHEMA = "path.tracking.schema";

  /**
   * Configure the input format to use the specified schema and optional path field.
   */
  public static void configure(Job job, FileSourceProperties properties) {
    Configuration conf = job.getConfiguration();
    String pathField = properties.getPathField();
    if (pathField != null) {
      conf.set(PATH_FIELD, pathField);
    }
    conf.setBoolean(FILENAME_ONLY, properties.useFilenameAsPath());
    FileFormat format = properties.getFormat();
    conf.set(FORMAT, format.name());
    Schema schema = properties.getSchema();

    if (schema != null) {
      conf.set(SCHEMA, schema.toString());
      if (format == FileFormat.AVRO) {
        AvroJob.setInputKeySchema(job, new org.apache.avro.Schema.Parser().parse(schema.toString()));
      } else if (format == FileFormat.PARQUET) {
        AvroWriteSupport.setSchema(conf, new org.apache.avro.Schema.Parser().parse(schema.toString()));
      }
    } else if (format == FileFormat.TEXT) {
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

  private static Schema addFieldToSchema(Schema schema, String addFieldKey) {
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
    Configuration hConf = context.getConfiguration();
    String pathField = hConf.get(PATH_FIELD);
    boolean filenameOnly = hConf.getBoolean(FILENAME_ONLY, false);
    String format = hConf.get(FORMAT);
    String path = filenameOnly ? fileSplit.getPath().getName() : fileSplit.getPath().toUri().toString();
    String schema = hConf.get(SCHEMA);
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
      // this is set by CombinePathTrackingInputFormat.createRecordReader()
      String header = hConf.get(CombinePathTrackingInputFormat.HEADER);
      RecordReader<LongWritable, Text> delegate = (new TextInputFormat()).createRecordReader(split, context);
      return new TrackingTextRecordReader(delegate, pathField, path, parsedSchema, header);
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
    public NullWritable getCurrentKey() {
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
    private boolean emittedHeader;
    private String header;

    private TrackingTextRecordReader(RecordReader<LongWritable, Text> delegate, @Nullable String pathField,
                                     String path, Schema schema, @Nullable String header) {
      super(delegate, pathField, path, schema);
      this.header = header;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      super.initialize(split, context);
      emittedHeader = false;
    }

    public StructuredRecord.Builder startCurrentValue() throws IOException, InterruptedException {
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
  }

  private static class TrackingAvroRecordReader extends TrackingRecordReader<AvroKey<GenericRecord>, NullWritable> {
    private static AvroToStructuredTransformer recordTransformer;

    private TrackingAvroRecordReader(RecordReader<AvroKey<GenericRecord>, NullWritable> delegate,
                                     @Nullable String pathField, String path, Schema schema) {
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
                                        String path, Schema schema) {
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
