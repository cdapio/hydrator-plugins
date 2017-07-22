/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.file;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import org.apache.hadoop.io.NullWritable;

/**
 * Abstract class for FileCopySource plugin. Extracts metadata of desired files
 * from the source database.
 * @param <K> the FileMetadata class specific to each database.
 */
public abstract class AbstractFileMetadataSource<K extends AbstractFileMetadata>
  extends ReferenceBatchSource<NullWritable, K, StructuredRecord> {
  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "metadata",
    Schema.Field.of(AbstractFileMetadata.FILE_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(AbstractFileMetadata.FULL_PATH, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(AbstractFileMetadata.FILE_SIZE, Schema.of(Schema.Type.LONG)),
    Schema.Field.of(AbstractFileMetadata.TIMESTAMP, Schema.of(Schema.Type.LONG)),
    Schema.Field.of(AbstractFileMetadata.OWNER, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(AbstractFileMetadata.IS_FOLDER, Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of(AbstractFileMetadata.BASE_PATH, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(AbstractFileMetadata.PERMISSION, Schema.of(Schema.Type.INT))
  );

  private final AbstractFileMetadataSourceConfig config;

  public AbstractFileMetadataSource(AbstractFileMetadataSourceConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Loads configurations from UI and check if they are valid.
   */
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    this.config.validate();
  }

  /**
   * Initialize the output StructuredRecord Schema here.
   * @param context
   * @throws Exception
   */
  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  /**
   * Load job configurations here.
   */
  public abstract void prepareRun(BatchSourceContext context) throws Exception;

  /**
   * Convert file metadata to StructuredRecord and emit.
   */
  public abstract void transform(KeyValue<NullWritable, K> input, Emitter<StructuredRecord> emitter);

  /**
   * Abstract class for the configuration of FileCopySink
   */
  public abstract class AbstractFileMetadataSourceConfig extends ReferencePluginConfig {

    @Macro
    @Description("Collection of sourcePaths separated by \",\" to read files from")
    public String sourcePaths;

    @Macro
    @Description("The number of files each split manipulates")
    public int maxSplitSize;

    @Description("Whether or not to copy recursively")
    public Boolean recursiveCopy;

    public AbstractFileMetadataSourceConfig(String name, String sourcePaths, int maxSplitSize) {
      super(name);
      this.sourcePaths = sourcePaths;
      this.maxSplitSize = maxSplitSize;
    }

    public void validate() {
      }
    }

    /*
     * Put additional configurations here for specific databases.
     */
}
