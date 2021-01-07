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

package io.cdap.plugin.format.delimited.input;

import com.google.common.base.Splitter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * Delimited text format that tracks which file each record was read from.
 */
public class PathTrackingDelimitedInputFormat extends PathTrackingInputFormat {
  static final String DELIMITER = "delimiter";
  static final String SKIP_HEADER = "skip_header";

  @Override
  protected RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(FileSplit split,
                                                                                    TaskAttemptContext context,
                                                                                    @Nullable String pathField,
                                                                                    @Nullable Schema schema) {

    RecordReader<LongWritable, Text> delegate = getDefaultRecordReaderDelegate(split, context);
    String delimiter = context.getConfiguration().get(DELIMITER);
    boolean skipHeader = context.getConfiguration().getBoolean(SKIP_HEADER, false);

    return new RecordReader<NullWritable, StructuredRecord.Builder>() {

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        delegate.initialize(split, context);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (delegate.nextKeyValue()) {
          // skip to next if the current record is header
          if (skipHeader && delegate.getCurrentKey().get() == 0L) {
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
        String delimitedString = delegate.getCurrentValue().toString();

        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        Iterator<Schema.Field> fields = schema.getFields().iterator();

        for (String part : Splitter.on(delimiter).split(delimitedString)) {
          if (!fields.hasNext()) {
            int numDataFields = delimitedString.split(delimiter).length;
            int numSchemaFields = schema.getFields().size();
            String message = String.format("Found a row with %d fields when the schema only contains %d field%s.",
                                           numDataFields, numSchemaFields, numSchemaFields == 1 ? "" : "s");
            // special error handling for the case when the user most likely set the schema to delimited
            // when they meant to use 'text'.
            Schema.Field bodyField = schema.getField("body");
            if (bodyField != null) {
              Schema bodySchema = bodyField.getSchema();
              bodySchema = bodySchema.isNullable() ? bodySchema.getNonNullable() : bodySchema;
              if (bodySchema.getType() == Schema.Type.STRING) {
                throw new IOException(message + " Did you mean to use the 'text' format?");
              }
            }
            throw new IOException(message + " Check that the schema contains the right number of fields.");
          }

          if (part.isEmpty()) {
            builder.set(fields.next().getName(), null);
          } else {
            builder.convertAndSet(fields.next().getName(), part);
          }
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
    };
  }
}
