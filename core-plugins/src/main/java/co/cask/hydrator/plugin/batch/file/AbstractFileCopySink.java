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
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.plugin.batch.file.s3.S3FileMetadata;
import co.cask.hydrator.plugin.batch.file.s3.S3FileMetadata.S3Credentials;
import com.sun.istack.Nullable;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Abstract template for a FileCopySink. The transform method converts a structured record
 * to AbstractFileMetadata class.
 */
public abstract class AbstractFileCopySink
  extends ReferenceBatchSink<StructuredRecord, NullWritable, AbstractFileMetadata> {
  protected final AbstractFileCopySinkConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileCopySink.class);

  public AbstractFileCopySink(AbstractFileCopySinkConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Converts input StructuredRecord to AbstractFileMetadata class. Loads credentials and
   * file metadata from the input.
   * @param input The input structured record that contains credentials and file metadata.
   * @param emitter
   * @throws Exception
   */
  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, AbstractFileMetadata>> emitter)
    throws Exception {
    AbstractFileMetadata output;
    switch ((String) input.get(AbstractFileMetadata.Credentials.DATA_BASE_TYPE)) {
      case S3FileMetadata.DATA_BASE_NAME :
        output = new S3FileMetadata((String) input.get(AbstractFileMetadata.FILE_NAME),
                                    (String) input.get(AbstractFileMetadata.FULL_PATH),
                                    (long) input.get(AbstractFileMetadata.TIMESTAMP),
                                    (String) input.get(AbstractFileMetadata.OWNER),
                                    (long) input.get(AbstractFileMetadata.FILE_SIZE),
                                    (Boolean) input.get(AbstractFileMetadata.IS_FOLDER),
                                    (String) input.get(AbstractFileMetadata.BASE_PATH),
                                    (short) input.get(AbstractFileMetadata.PERMISSION),
                                    new S3FileMetadata.S3Credentials((String) input.get(S3Credentials.ACCESS_KEY_ID),
                                                                     (String) input.get(S3Credentials.SECRET_KEY_ID),
                                                                     (String) input.get(S3Credentials.REGION),
                                                                     (String) input.get(S3Credentials.BUCKET_NAME))

          );
        break;

      default:
        throw new IllegalArgumentException("unrecognized database type");

    }
    emitter.emit(new KeyValue<NullWritable, AbstractFileMetadata>(null, output));
  }

  /**
   * Abstract class for the configuration of FileCopySink
   */
  public abstract class AbstractFileCopySinkConfig extends ReferencePluginConfig {

    @Macro
    @Description("The destination path. Will be created if it doesn't exist.")
    public String basePath;

    @Description("Whether or not to overwrite if the file already exists.")
    public Boolean enableOverwrite;

    @Macro
    @Nullable
    @Description("The size of the buffer that temporarily stores data from file input stream.")
    public int bufferSize;

    public AbstractFileCopySinkConfig(String name, String basePath, Boolean enableOverwrite, @Nullable int bufferSize) {
      super(name);
      this.basePath = basePath;
      this.enableOverwrite = enableOverwrite;
      this.bufferSize = bufferSize;
    }
    /*
     * Additional configurations for the file sink should be implemented in the extended class
     */
  }

  /**
   * Adds necessary configuration resources and provides OutputFormat Class
   */
  public class FileCopyOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    /**
     *
     * @param config
     */
    public FileCopyOutputFormatProvider(AbstractFileCopySink.AbstractFileCopySinkConfig config) {
      this.conf = new HashMap<>();
      FileCopyOutputFormat.setBasePath(conf, config.basePath);
      FileCopyOutputFormat.setEnableOverwrite(conf, config.enableOverwrite.toString());
      FileCopyOutputFormat.setBufferSize(conf, String.valueOf(config.bufferSize));
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }

    @Override
    public String getOutputFormatClassName() {
      return FileCopyOutputFormat.class.getName();
    }
  }

}
