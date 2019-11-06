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
import co.cask.hydrator.format.OrcToStructuredTransformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Reads parquet into StructuredRecords.
 */
public class OrcInputFormatter implements FileInputFormatter {
  private final Schema schema;

  OrcInputFormatter(@Nullable Schema schema) {
    this.schema = schema;
  }

  @Override
  public Map<String, String> getFormatConfig() {
    Map<String, String> properties = new HashMap<>();
    if (schema != null) {
      // properties.put("parquet.avro.schema", schema.toString());
    }
    return properties;
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord.Builder> create(FileSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration hConf = context.getConfiguration();
    String pathField = hConf.get(PathTrackingInputFormat.PATH_FIELD);
    RecordReader<NullWritable, OrcStruct> delegate = (new OrcInputFormat<OrcStruct>())
      .createRecordReader(split, context);
    return new OrcRecordReader(delegate, schema, pathField);
  }

  /**
   * Transforms OrcStruct into StructuredRecord.
   */
  static class OrcRecordReader extends RecordReader<NullWritable, StructuredRecord.Builder> {
    private final RecordReader<NullWritable, OrcStruct> delegate;
    private final OrcToStructuredTransformer recordTransformer;
    private final String pathField;
    private Schema schema;

    OrcRecordReader(RecordReader<NullWritable, OrcStruct> delegate, @Nullable Schema schema,
                        @Nullable String pathField) {
      this.delegate = delegate;
      this.schema = schema;
      this.pathField = pathField;
      this.recordTransformer = new OrcToStructuredTransformer();
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
      OrcStruct orcStruct = delegate.getCurrentValue();
      // if schema is null, but we're still able to read, that means the file contains the schema information
      // set the schema based on the schema of the record
      if (schema == null) {
        if (pathField == null) {
          schema = recordTransformer.convertSchema(orcStruct.getSchema());
        } else {
          // if there is a path field, add the path as a field in the schema
          Schema schemaWithoutPath = recordTransformer.convertSchema(orcStruct.getSchema());
          List<Schema.Field> fields = new ArrayList<>(schemaWithoutPath.getFields().size() + 1);
          fields.addAll(schemaWithoutPath.getFields());
          fields.add(Schema.Field.of(pathField, Schema.of(Schema.Type.STRING)));
          schema = Schema.recordOf(schemaWithoutPath.getRecordName(), fields);
        }
      }
      return recordTransformer.transform(orcStruct, schema, pathField);
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
