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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * {@link WindowsShareCopy} is an {@link Action} that will copy the data from a Windows share into an HDFS directory.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("WindowsShareCopy")
@Description("Copies a file or files on a Microsoft Windows share to an HDFS directory.")
public class WindowsShareCopy extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(WindowsShareCopy.class);
  private static final int MIN_BUFFER_SIZE = 4096;
  private static final int MIN_NUM_THREADS = 1;
  private WindowsShareCopyConfig config;

  public WindowsShareCopy(WindowsShareCopyConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    config.numThreads = (config.numThreads == null || config.numThreads < MIN_NUM_THREADS) ? MIN_NUM_THREADS :
      config.numThreads;
    config.bufferSize = (config.bufferSize == null || config.bufferSize < MIN_BUFFER_SIZE) ? MIN_BUFFER_SIZE :
      config.bufferSize;
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
    final String smbDirectory = sb.toString();

    // Register the SMB File handler.
    jcifs.Config.registerSmbURLHandler();
    // Set Jcifs Log level to log debug information also
    jcifs.Config.setProperty("jcifs.util.loglevel", "4");

    // Authentication with NTLM and read the directory from the Windows Share.
    final NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(config.netBiosDomainName,
                                                                           config.netBiosUsername,
                                                                           config.netBiosPassword);
    SmbFile dir = new SmbFile(smbDirectory, auth);

    // Determines the buffer size to be used during writing.
    Configuration conf = new Configuration();
    conf.setLong("io.file.buffer.size", config.bufferSize);
    conf.setLong("file.stream-buffer-size", config.bufferSize);
    // Create the HDFS directory if it doesn't exist.
    final Path hdfsDir = new Path(config.destinationDirectory);
    final FileSystem hdfs = hdfsDir.getFileSystem(conf);
    if (!hdfs.exists(hdfsDir)) {
      hdfs.mkdirs(hdfsDir);
    }

    String[] files = dir.list();
    // Copies the files in a multithreaded way
    CountDownLatch executorTerminateLatch = new CountDownLatch(1);
    ExecutorService executorService = createExecutor(config.numThreads, executorTerminateLatch);
    CompletionService<String> completionService = new ExecutorCompletionService<>(executorService);

    try {
      for (final String file : files) {
        completionService.submit(new Callable<String>() {
          @Override
          public String call() throws Exception {
            try {
              if (smbDirectory.endsWith("/")) {
                return copyFileToHDFS(hdfs, smbDirectory + file, hdfsDir, auth);
              } else {
                return copyFileToHDFS(hdfs, smbDirectory + "/" + file, hdfsDir, auth);
              }
            } catch (Exception e) {
              LOG.warn("Exception while copying the file {}", file, e);
              return null;
            }
          }
        });
      }

      int count = 0;
      while (count < files.length) {
        try {
          Future<String> fileWritten = completionService.take();
          String fileName = fileWritten.get();
          if (fileName != null) {
            LOG.debug("{} is copied", fileName);
          }
        } catch (Throwable t) {
          throw Throwables.propagate(t);
        }
        count++;
      }
    } finally {
      executorService.shutdownNow();
      executorTerminateLatch.await();
    }
  }

  private String copyFileToHDFS(FileSystem hdfs, String smbSourceFile, Path dest, NtlmPasswordAuthentication auth)
    throws IOException {
    SmbFile smbFile = new SmbFile(smbSourceFile, auth);
    String name = smbFile.getName();
    Path destFile = new Path(dest, name);
    LOG.debug("Thread {} is copying source file {}, dest file {}", Thread.currentThread().getName(),
              smbFile.getName(), destFile.getName());
    // If file already exists, then we skip over.
    if (hdfs.exists(destFile) && !config.overwrite) {
      LOG.info("File {} already exists on HDFS, Skipping", destFile.getName());
      return null;
    }
    LOG.info("Copying file {} to {}", smbSourceFile, destFile.toString());
    try (InputStream in = smbFile.getInputStream();
         BufferedOutputStream out = new BufferedOutputStream(hdfs.create(destFile), config.bufferSize)) {
      ByteStreams.copy(in, out);
    } catch (IOException e) {
      LOG.warn("Exception in copying the file {}", name, e);
    }
    return name;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.netBiosHostname),
                                "NetBios hostname should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.netBiosUsername),
                                "NetBios username should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.netBiosPassword),
                                "NetBios password should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.netBiosSharename),
                                "NetBios share name should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.sourceDirectory),
                                "NetBios source directory should be non-null, non-empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(config.destinationDirectory),
                                "HDFS destination directory should be non-null, non-empty.");
  }

  /**
   * Creates an {@link ExecutorService} that has the given number of threads.
   *
   * @param threads number of core threads in the executor
   * @param terminationLatch a {@link CountDownLatch} that will be counted down when the executor terminated
   * @return a new {@link ExecutorService}.
   */
  private ExecutorService createExecutor(int threads, final CountDownLatch terminationLatch) {
    return new ThreadPoolExecutor(
      threads, threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
      new ThreadFactoryBuilder().build()) {
      @Override
      protected void terminated() {
        terminationLatch.countDown();
      }
    };
  }

  /**
   * Config class that contains all properties necessary to execute an HDFS move command.
   */
  public class WindowsShareCopyConfig extends PluginConfig {

    @Description("Specifies the NetBios domain name.")
    @Nullable
    @Macro
    private final String netBiosDomainName;

    @Description("Specifies the NetBios hostname to import files from.")
    @Macro
    private final String netBiosHostname;

    @Description("Specifies the NetBios username to use when importing files from the Windows share.")
    @Macro
    private final String netBiosUsername;

    @Description("Specifies the NetBios password to use when importing files from the Windows share.")
    @Macro
    private final String netBiosPassword;

    @Description("Specifies the NetBios share name.")
    @Macro
    private final String netBiosSharename;

    @Description("Specifies the number of parallel tasks to use when executing the copy operation; defaults to 1.")
    @Nullable
    @Macro
    private Integer numThreads;

    @Description("Boolean that specifies if any matching files already present in the destination " +
      "should be overwritten or not; default is true.")
    @Nullable
    private final Boolean overwrite;

    @Description("Specifies the NetBios directory or file.")
    @Macro
    private final String sourceDirectory;

    @Description("The valid full HDFS destination path in the same cluster where " +
      "the file or files are to be moved. If a directory is specified as a destination with a " +
      "file as the source, the source file will be put into that directory. If the source is a " +
      "directory, it is assumed that destination is also a directory. This plugin does not check " +
      "and will not catch any inconsistency.")
    @Macro
    private final String destinationDirectory;

    @Description("The size of the buffer to be used for copying the files; minimum (and " +
      "default) buffer size is 4096; the value should be a multiple of the minimum size.")
    @Nullable
    @Macro
    private Integer bufferSize;

    WindowsShareCopyConfig(String netBiosDomainName, String netBiosHostname, String netBiosUsername,
                           String netBiosPassword, String netBiosSharename, String sourceDirectory,
                           String destinationDirectory, Integer bufferSize, Integer numThreads, String overwrite) {

      this.netBiosDomainName = netBiosDomainName;
      this.netBiosHostname = netBiosHostname;
      this.netBiosUsername = netBiosUsername;
      this.netBiosPassword = netBiosPassword;
      this.netBiosSharename = netBiosSharename;
      this.sourceDirectory = sourceDirectory;
      this.destinationDirectory = destinationDirectory;
      this.bufferSize = bufferSize;
      this.numThreads = numThreads;
      this.overwrite = !("false".equals(overwrite));
    }
  }
}
