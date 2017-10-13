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

package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.hydrator.plugin.batch.source.FileBatchSource;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * FTP Source Test.
 */
public class ETLFTPTestRun extends ETLBatchTestBase {

  private static final String USER = "ftp";
  private static final String PWD = "abcd";
  private static final String TEST_STRING = "Hello World";

  public static File folder;
  public static File file;
  public static int port;

  private static FakeFtpServer ftpServer;

  @Before
  public void init() throws Exception {
    folder = TMP_FOLDER.newFolder();
    file = new File(folder, "sample");

    ftpServer = new FakeFtpServer();
    ftpServer.setServerControlPort(0);

    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry(file.getAbsolutePath(), TEST_STRING));
    ftpServer.setFileSystem(fileSystem);

    ftpServer.addUserAccount(new UserAccount(USER, PWD, folder.getAbsolutePath()));
    ftpServer.start();

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return ftpServer.isStarted();
      }
    }, 5, TimeUnit.SECONDS);
    port = ftpServer.getServerControlPort();
  }

  @After
  public void stop() throws Exception {
    if (ftpServer != null) {
      ftpServer.stop();
    }
  }

  @Test
  public void testFTPBatchSource() throws Exception {
    ETLStage source = new ETLStage("source", new ETLPlugin(
      "FTP",
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(Properties.File.PATH, String.format("ftp://%s:%s@localhost:%d%s",
                                                 USER, PWD, port, folder.getAbsolutePath()))
        .put(Properties.File.IGNORE_NON_EXISTING_FOLDERS, "false")
        .put("referenceName", "ftp")
        .build(),
      null));
    ETLStage sink = new ETLStage("sink", new ETLPlugin(
      "TPFSAvro",
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(Properties.TimePartitionedFileSetDataset.SCHEMA, FileBatchSource.DEFAULT_SCHEMA.toString())
        .put(Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink").build(),
      null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationManager appManager = deployETL(etlConfig, "FTPToTPFS");
    runETLOnce(appManager);

    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("fileSink");
    try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
      List<GenericRecord> records = readOutput(fileSet, FileBatchSource.DEFAULT_SCHEMA);
      Assert.assertEquals(1, records.size());
      GenericRecord record = records.get(0);
      Assert.assertEquals(TEST_STRING, record.get("body").toString());
    }
  }
}
