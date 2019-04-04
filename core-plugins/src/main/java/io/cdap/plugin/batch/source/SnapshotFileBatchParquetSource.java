/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.plugin.common.FileSetUtil;
import io.cdap.plugin.format.FileFormat;
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
