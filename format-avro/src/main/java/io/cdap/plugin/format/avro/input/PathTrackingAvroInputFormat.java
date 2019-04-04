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

package io.cdap.plugin.format.avro.input;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.avro.AvroToStructuredTransformer;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Avro format that tracks which file each record was read from.
 */
public class PathTrackingAvroInputFormat extends PathTrackingInputFormat {

  @Override
  protected RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(
    FileSplit split, TaskAttemptContext context,
    @Nullable String pathField, @Nullable Schema schema) throws IOException, InterruptedException {

    RecordReader<AvroKey<GenericRecord>, NullWritable> delegate = (new AvroKeyInputFormat<GenericRecord>())
      .createRecordReader(split, context);
    return new AvroRecordReader(delegate, schema, pathField);
  }

  /**
   * Transforms GenericRecords into StructuredRecord.
   */
  static class AvroRecordReader extends RecordReader<NullWritable, StructuredRecord.Builder> {
    private final RecordReader<AvroKey<GenericRecord>, NullWritable> delegate;
    private final AvroToStructuredTransformer recordTransformer;
    private final String pathField;
    private Schema schema;

    AvroRecordReader(RecordReader<AvroKey<GenericRecord>, NullWritable> delegate, @Nullable Schema schema,
                     @Nullable String pathField) {
      this.delegate = delegate;
      this.schema = schema;
      this.pathField = pathField;
      this.recordTransformer = new AvroToStructuredTransformer();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return delegate.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() {
      return NullWritable.get();
    }

    @Override
    public StructuredRecord.Builder getCurrentValue() throws IOException, InterruptedException {
      GenericRecord genericRecord = delegate.getCurrentKey().datum();
      // if schema is null, but we're still able to read, that means the file contains the schema information
      // set the schema based on the schema of the record
      if (schema == null) {
        if (pathField == null) {
          schema = Schema.parseJson(genericRecord.getSchema().toString());
        } else {
          // if there is a path field, add the path as a field in the schema
          Schema schemaWithoutPath = Schema.parseJson(genericRecord.getSchema().toString());
          List<Schema.Field> fields = new ArrayList<>(schemaWithoutPath.getFields().size() + 1);
          fields.addAll(schemaWithoutPath.getFields());
          fields.add(Schema.Field.of(pathField, Schema.of(Schema.Type.STRING)));
          schema = Schema.recordOf(schemaWithoutPath.getRecordName(), fields);
        }
      }
      return recordTransformer.transform(genericRecord, schema, pathField);
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
