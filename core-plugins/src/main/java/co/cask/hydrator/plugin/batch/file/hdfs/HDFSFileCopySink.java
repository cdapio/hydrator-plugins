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

package co.cask.hydrator.plugin.batch.file.hdfs;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.hydrator.plugin.batch.file.AbstractFileCopySink;
import com.sun.istack.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileCopySink that writes to HDFS.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("HDFSFileCopySink")
@Description("Copies files from remote filesystem to HDFS")
public class HDFSFileCopySink extends AbstractFileCopySink {

  private HDFSFileCopySinkConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(HDFSFileCopySink.class);

  public HDFSFileCopySink(HDFSFileCopySinkConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Configurations required for connecting to HDFS.
   */
  public class HDFSFileCopySinkConfig extends AbstractFileCopySinkConfig {
    public HDFSFileCopySinkConfig(String name, String basePath, Boolean enableOverwrite,
                                  Boolean preserveFileOwner, @Nullable Integer bufferSize) {
      super(name, basePath, enableOverwrite, preserveFileOwner, bufferSize);
    }

    @Override
    public String getScheme() {
      return "hdfs";
    }

    @Override
    public String getHostUri() {
      return null;
    }
  }
}
