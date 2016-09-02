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

package co.cask.hydrator.plugin.batch.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.annotations.VisibleForTesting;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;

/**
 * {@link WindowsShareCopy} is a {@link Action} that will copy the data from Window share into HDFS directory.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("WindowsShareCopy")
@Description("Copies Files from Windows Share into HDFS Directory without intermediate store.")
public class WindowsShareCopy extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(WindowsShareCopy.class);
  private static final long MIN_BUFFER_SIZE = 4096;

  private WindowsShareCopyConfig config;

  public WindowsShareCopy(WindowsShareCopyConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    StringBuilder sb = new StringBuilder("smb://");
    sb.append(config.netBiosHostname);
    sb.append("/");
    sb.append(config.netBiosSharename);
    sb.append("/");
    if (config.sourceDirectory.startsWith("/")) {
      if (config.sourceDirectory.length() > 1) {
        sb.append(config.sourceDirectory.substring(1));
      }
    } else {
      sb.append(config.sourceDirectory);
    }
    String smbDirectory = sb.toString();

    String domain = null;
    if (config.netBiosDomainName != null) {
      domain = config.netBiosDomainName;
    }

    // Register the SMB File handler.
    jcifs.Config.registerSmbURLHandler();
    jcifs.Config.setProperty("jcifs.util.loglevel", "4");

    // Authentication with NTLM and read the directory from the Windows Share.
    NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(domain, config.netBiosUsername,
                                                                     config.netBiosPassword);
    SmbFile dir = new SmbFile(smbDirectory, auth);

    // Create the HDFS directory if it doesn't exist.
    Path hdfsDir = new Path(config.destinationDirectory);
    FileSystem hdfs = hdfsDir.getFileSystem(new Configuration());
    if (!hdfs.exists(hdfsDir)) {
      hdfs.mkdirs(hdfsDir);
    }

    // Determines the buffer size to be used during writing.
    Configuration conf = new Configuration();
    conf.setLong("io.file.buffer.size", config.bufferSize);

    for (String file : dir.list()) {
      if (smbDirectory.endsWith("/")) {
        copyFileToHDFS(hdfs, smbDirectory + file, hdfsDir, auth);
      } else {
        copyFileToHDFS(hdfs, smbDirectory + "/" + file, hdfsDir, auth);
      }
    }
  }


  private void copyFileToHDFS(FileSystem hdfs, String smbSourceFile, Path dest, NtlmPasswordAuthentication auth) {
    InputStream in = null;
    BufferedOutputStream out = null;

    try {
      SmbFile fsmb = new SmbFile(smbSourceFile, auth);
      in = fsmb.getInputStream();
      String name = fsmb.getName();
      Path nDest = new Path(dest, name);

      LOG.info("Copying file {} to {}", smbSourceFile, nDest.toString());

      // If file already exists, then we skip over.
      if (hdfs.exists(nDest)) {
        LOG.warn("File {} already exists on HDFS, Skipping", nDest.getName());
        return;
      }

      out = new BufferedOutputStream(hdfs.create(nDest));
      IOUtils.copyBytes(in, out, 1024 * 1024 * 1024);
    } catch (MalformedURLException e) {
      LOG.warn("Failed to copy file {}", smbSourceFile, e);
    } catch (IOException e) {
      LOG.warn("Failed to copy file {}", smbSourceFile, e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          // Eat it up!
        }
      }
      if (out != null) {
        try {
          out.flush();
          out.close();
        } catch (IOException e) {
          // Eat it up!
        }
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {

  }

  /**
   * Config class that contains all properties necessary to execute an HDFS move command.
   */
  public class WindowsShareCopyConfig extends PluginConfig {

    @Description("Specifies the NetBios Domain name.")
    @Nullable
    @Macro
    private String netBiosDomainName;

    @Description("Specifies the NetBios Hostname to import files from")
    @Macro
    private String netBiosHostname;

    @Description("Specifies the NetBios username to user for importing files from share")
    @Macro
    private String netBiosUsername;

    @Description("Specifies the NetBios password for importing files from windows share")
    @Macro
    private String netBiosPassword;

    @Description("Specifies the NetBios share name")
    @Macro
    private String netBiosSharename;

    @Description("Specifies the NetBios directory")
    @Macro
    private String sourceDirectory;

    @Description("The valid, full HDFS destination path in the same cluster where the file or files are to be moved. " +
      "If a directory is specified with a file sourcePath, the file will be put into that directory. If sourcePath " +
      "is a directory, it is assumed that destPath is also a directory. HDFSAction will not catch this inconsistency.")
    @Macro
    private String destinationDirectory;

    @Description("Write buffer size")
    private long bufferSize;

    @VisibleForTesting
    WindowsShareCopyConfig(String netBiosDomainName, String netBiosHostname , String netBiosUsername,
                           String netBiosPassword, String netBiosSharename, String sourceDirectory,
                           String destinationDirectory, int bufferSize) {

      this.netBiosDomainName = netBiosDomainName;
      this.netBiosHostname = netBiosHostname;
      this.netBiosUsername = netBiosUsername;
      this.netBiosPassword = netBiosPassword;
      this.netBiosSharename = netBiosSharename;
      this.sourceDirectory = sourceDirectory;
      this.destinationDirectory = destinationDirectory;
      this.bufferSize = bufferSize < MIN_BUFFER_SIZE  ? MIN_BUFFER_SIZE : bufferSize;
    }
  }
}
