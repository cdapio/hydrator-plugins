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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.plugin.batch.file.s3.S3FileMetadata;
import com.sun.istack.Nullable;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
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

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
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
    String fsScheme = URI.create((String) input.get(AbstractFileMetadata.HOST_URI)).getScheme();
    switch (fsScheme) {
      case "s3n" :
      case "s3a" :
        output = new S3FileMetadata(input);
        break;

      default:
        throw new IllegalArgumentException(fsScheme + "is not supported.");

    }
    emitter.emit(new KeyValue<NullWritable, AbstractFileMetadata>(null, output));
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    config.validate();
    context.addOutput(Output.of(config.referenceName, new FileCopyOutputFormatProvider(config)));
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

    @Description("Whether or not to preserve the owner of the file from source filesystem.")
    public Boolean preserveFileOwner;

    // TODO: figure out why this still shows up as required in configuration UI
    @Macro
    @Nullable
    @Description("The size of the buffer (in MB) that temporarily stores data from file input stream. Defaults to" +
      " 1 MB")
    public Integer bufferSize;

    public AbstractFileCopySinkConfig(String name, String basePath, Boolean enableOverwrite,
                                      Boolean preserveFileOwner, @Nullable Integer bufferSize) {
      super(name);
      this.basePath = basePath;
      this.enableOverwrite = enableOverwrite;
      this.preserveFileOwner = preserveFileOwner;
      this.bufferSize = bufferSize;
    }

    public void validate() {
      if (!this.containsMacro("bufferSize")) {
        if (bufferSize <= 0) {
          throw new IllegalArgumentException("Buffer size must be a positive integer.");
        }
      }
    }

    /*
     * Additional configurations for the file sink should be implemented in the extended class
     */

    public abstract String getScheme();

    public abstract String getHostUri();
  }

  /**
   * Adds necessary configuration resources and provides OutputFormat Class
   */
  public class FileCopyOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    public FileCopyOutputFormatProvider(AbstractFileCopySink.AbstractFileCopySinkConfig config) {
      this.conf = new HashMap<>();
      FileCopyOutputFormat.setBasePath(conf, config.basePath);
      FileCopyOutputFormat.setEnableOverwrite(conf, config.enableOverwrite.toString());
      FileCopyOutputFormat.setPreserveFileOwner(conf, config.preserveFileOwner.toString());

      if (config.bufferSize != null) {
        // bufferSize is in megabytes
        FileCopyOutputFormat.setBufferSize(conf, String.valueOf(config.bufferSize << 20));
      } else {
        FileCopyOutputFormat.setBufferSize(conf, String.valueOf(FileCopyRecordWriter.DEFAULT_BUFFER_SIZE));
      }

      // always disable caching
      conf.put(String.format("fs.%s.impl.disable.cache", config.getScheme()), String.valueOf(true));

      // set the URI for the destination filesystem if it's provided
      if (config.getHostUri() != null) {
        FileCopyOutputFormat.setFilesystemHostUri(conf, config.getHostUri());
      }
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
