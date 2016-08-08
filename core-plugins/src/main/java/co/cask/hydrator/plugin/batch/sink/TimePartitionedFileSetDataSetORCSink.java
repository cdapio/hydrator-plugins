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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.hydrator.common.HiveSchemaConverter;
import co.cask.hydrator.plugin.common.FileSetUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write ORC records to a {@link TimePartitionedFileSet}.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("TPFSOrc")
@Description("Sink for a TimePartitionedFileSet that writes data in ORC format.")
public class TimePartitionedFileSetDataSetORCSink extends TimePartitionedFileSetSink<NullWritable, OrcStruct> {

  private static final String SCHEMA_DESC = "The ORC schema of the record being written to the Sink.";
  private static final Logger LOG = LoggerFactory.getLogger(TimePartitionedFileSetDataSetORCSink.class);
  private final TPFSOrcSinkConfig config;

  public TimePartitionedFileSetDataSetORCSink(TPFSOrcSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  protected void addFileSetProperties(FileSetProperties.Builder properties) {
    try {
      FileSetUtil.configureORCFileSet(config.schema, properties);
    } catch (IOException e) {
      LOG.debug("");
    } catch (UnsupportedTypeException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, OrcStruct>> emitter) throws Exception {

    StringBuilder builder = new StringBuilder();
    HiveSchemaConverter.appendType(builder, input.getSchema());
    TypeDescription schema = TypeDescription.fromString(builder.toString());
    OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);
    List<Schema.Field> fields = input.getSchema().getFields();

    //populate ORC struct Pair object
    //TODO Write schema transformer to support complex objects
    for (int i = 0; i < fields.size(); i++) {
      String fieldval = input.get(fields.get(i).getName());
      pair.setFieldValue(fields.get(i).getName(), new Text(fieldval));
    }
    emitter.emit(new KeyValue<NullWritable, OrcStruct>(NullWritable.get(), pair));
  }

  /**
   * Config for TimePartitionedFileSetOrcSink
   */
  public static class TPFSOrcSinkConfig extends TPFSSinkConfig {

    @Description(SCHEMA_DESC)
    private String schema;

    public TPFSOrcSinkConfig(String name, String schema, @Nullable String basePath, @Nullable String pathFormat,
                             @Nullable String timeZone) {
      super(name, basePath, pathFormat, timeZone);
      this.schema = schema;
    }
  }
}
