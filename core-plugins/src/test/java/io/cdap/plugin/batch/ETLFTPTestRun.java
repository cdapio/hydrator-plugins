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

package io.cdap.plugin.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.batch.source.FTPBatchSource;
import io.cdap.plugin.common.Properties;
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
import java.util.concurrent.TimeUnit;

/**
 * FTP Source Test.
 */
public class ETLFTPTestRun extends ETLBatchTestBase {

  private static final String USER = "ftp";
  private static final String PWD = "abcd";
  private static final String TEST_STRING = "Hello World";

  private static File folder;
  private static int port;

  private static FakeFtpServer ftpServer;

  @Before
  public void init() throws Exception {
    folder = TMP_FOLDER.newFolder();
    File file = new File(folder, "sample");

    ftpServer = new FakeFtpServer();
    ftpServer.setServerControlPort(0);

    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry(file.getAbsolutePath(), TEST_STRING));
    ftpServer.setFileSystem(fileSystem);

    ftpServer.addUserAccount(new UserAccount(USER, PWD, folder.getAbsolutePath()));
    ftpServer.start();

    Tasks.waitFor(true, () -> ftpServer.isStarted(), 5, TimeUnit.SECONDS);
    port = ftpServer.getServerControlPort();
  }

  @After
  public void stop() {
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
        .put(Properties.TimePartitionedFileSetDataset.SCHEMA, FTPBatchSource.SCHEMA.toString())
        .put(Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink").build(),
      null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationManager appManager = deployETL(etlConfig, "FTPToTPFS");
    runETLOnce(appManager);

    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("fileSink");
    try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
      List<GenericRecord> records = readOutput(fileSet, FTPBatchSource.SCHEMA);
      Assert.assertEquals(1, records.size());
      GenericRecord record = records.get(0);
      Assert.assertEquals(TEST_STRING, record.get("body").toString());
    }
  }
}
