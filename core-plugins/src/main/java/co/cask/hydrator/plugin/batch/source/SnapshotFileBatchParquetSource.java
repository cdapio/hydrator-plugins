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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.Requirements;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.hydrator.format.FileFormat;
import co.cask.hydrator.plugin.common.FileSetUtil;
import co.cask.hydrator.plugin.common.SnapshotFileSetConfig;
import org.apache.hadoop.io.NullWritable;

/**
 * Reads data written by a {@link SnapshotFileBatchParquetSource}. Reads only the most recent partition.
 */
@Plugin(type = "batchsource")
@Name("SnapshotParquet")
@Description("Reads the most recent snapshot that was written to a SnapshotParquet sink.")
@Requirements(datasetTypes = PartitionedFileSet.TYPE)
public class SnapshotFileBatchParquetSource extends SnapshotFileBatchSource<SnapshotFileSetSourceConfig> {

  public SnapshotFileBatchParquetSource(SnapshotFileSetSourceConfig config) {
    super(config);
  }

  @Override
  protected String getInputFormatName() {
    return FileFormat.PARQUET.name().toLowerCase();
  }

  @Override
  public void transform(KeyValue<NullWritable, StructuredRecord> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue());
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    FileSetUtil.configureParquetFileSet(config.getSchema().toString(), propertiesBuilder);
  }
}
