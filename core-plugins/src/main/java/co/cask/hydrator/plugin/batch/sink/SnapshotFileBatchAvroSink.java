/*
 * Copyright © 2015, 2016 Cask Data, Inc.
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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.Requirements;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.hydrator.format.FileFormat;
import co.cask.hydrator.plugin.common.FileSetUtil;

import java.io.IOException;
import javax.annotation.Nullable;


/**
 * {@link SnapshotFileBatchSink} that stores data in Avro format.
 */
@Plugin(type = "batchsink")
@Name("SnapshotAvro")
@Description("Sink for a SnapshotFileSet that writes data in Avro format.")
@Requirements(datasetTypes = PartitionedFileSet.TYPE)
public class SnapshotFileBatchAvroSink extends SnapshotFileBatchSink<SnapshotFileBatchAvroSink.Conf> {
  private final Conf config;

  public SnapshotFileBatchAvroSink(Conf config) {
    super(config);
    this.config = config;
  }

  @Override
  protected String getOutputFormatPlugin() {
    return FileFormat.AVRO.name().toLowerCase();
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    FileSetUtil.configureAvroFileSet(config.schema, propertiesBuilder);
  }

  /**
   * Config for SnapshotFileBatchAvroSink
   */
  public static class Conf extends SnapshotFileSetBatchSinkConfig {
    @Description("The Avro schema of the record being written to the Sink as a JSON Object.")
    @Macro
    private String schema;

    @Nullable
    @Description("Used to specify the compression codec to be used for the final dataset.")
    private String compressionCodec;

    @Override
    public void validate() {
      super.validate();
      getSchema();
    }

    @Nullable
    public Schema getSchema() {
      try {
        return schema == null ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
      }
    }
  }
}
