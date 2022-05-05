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

package io.cdap.plugin.format.json.input;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.SchemaValidator;
import io.cdap.plugin.format.MetadataField;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Json format that tracks which file each record was read from.
 */
public class PathTrackingJsonInputFormat extends PathTrackingInputFormat {


  private Schema getModifiedSchema(Schema schema, List<String> metadataFields) {
    // if the metadata fields (path, length, and modification time fields) are set, they might not be nullable
    // if they are not nullable, decoding a value into a StructuredRecord will fail because a non-nullable
    // field will have a null value.
    // so in these cases, a modified schema is used where the metadata fields are nullable
    if (metadataFields.isEmpty()) {
      return schema;
    }
    List<Schema.Field> fieldCopies = new ArrayList<>(schema.getFields().size());
    for (Schema.Field field : schema.getFields()) {
      if (metadataFields.contains(field.getName()) && !field.getSchema().isNullable()) {
        fieldCopies.add(Schema.Field.of(field.getName(), Schema.nullableOf(field.getSchema())));
      } else {
        fieldCopies.add(field);
      }
    }
    return Schema.recordOf(schema.getRecordName(), fieldCopies);
  }


  @Override
  protected RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(FileSplit split,
                                                                                    TaskAttemptContext context,
                                                                                    @Nullable String pathField,
                                                                                    @Nullable Schema schema) {
    RecordReader<LongWritable, Text> delegate = getDefaultRecordReaderDelegate(split, context);
    Schema modifiedSchema = getModifiedSchema(schema, Arrays.asList(pathField));

    return new JsonRecordReader(delegate, modifiedSchema);
  }


  @Override
  protected RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(FileSplit split,
                                                                                    TaskAttemptContext context,
                                                                                    @Nullable String pathField,
                                                                                    Map<String, MetadataField>
                                                                                              metadataFields,
                                                                                    @Nullable Schema schema) {
    RecordReader<LongWritable, Text> delegate = getDefaultRecordReaderDelegate(split, context);

    List<String> toNullableFields = new ArrayList<>(metadataFields.keySet());
    toNullableFields.add(pathField);
    Schema modifiedSchema = getModifiedSchema(schema, toNullableFields);

    return new JsonRecordReader(delegate, modifiedSchema);
  }

  private class JsonRecordReader extends RecordReader<NullWritable, StructuredRecord.Builder> {
    private final RecordReader<LongWritable, Text> delegate;
    private final Schema modifiedSchema;

    private JsonRecordReader(RecordReader<LongWritable, Text> delegate, Schema modifiedSchema) {
      this.delegate = delegate;
      this.modifiedSchema = modifiedSchema;
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
      String json = delegate.getCurrentValue().toString();
      StructuredRecord record = StructuredRecordStringConverter.fromJsonString(json, modifiedSchema);
      StructuredRecord.Builder builder = StructuredRecord.builder(modifiedSchema);
      for (Schema.Field field : modifiedSchema.getFields()) {
        Object value = record.get(field.getName());
        SchemaValidator.validateDateTimeField(field.getSchema(), field.getName(), value);
        builder.set(field.getName(), value);
      }
      return builder;
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
