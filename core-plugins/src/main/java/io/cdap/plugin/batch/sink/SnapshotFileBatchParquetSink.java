/*
 * Copyright © 2015, 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.plugin.common.FileSetUtil;
import io.cdap.plugin.format.FileFormat;

import java.io.IOException;
import javax.annotation.Nullable;


/**
 * {@link SnapshotFileBatchSink} that stores data in Parquet format.
 */
@Plugin(type = "batchsink")
@Name("SnapshotParquet")
@Description("Sink for a SnapshotFileSet that writes data in Parquet format.")
@Requirements(datasetTypes = PartitionedFileSet.TYPE)
public class SnapshotFileBatchParquetSink extends SnapshotFileBatchSink<SnapshotFileBatchParquetSink.Conf> {
  private final Conf config;

  public SnapshotFileBatchParquetSink(Conf config) {
    super(config);
    this.config = config;
  }

  @Override
  protected String getOutputFormatPlugin() {
    return FileFormat.PARQUET.name().toLowerCase();
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    FileSetUtil.configureParquetFileSet(config.schema, propertiesBuilder);
  }

  /**
   * Config for SnapshotFileBatchParquetSink.
   */
  public static class Conf extends SnapshotFileSetBatchSinkConfig {
    @Description("The Parquet schema of the record being written to the Sink as a JSON Object.")
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
