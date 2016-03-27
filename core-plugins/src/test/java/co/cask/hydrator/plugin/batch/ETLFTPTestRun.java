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
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.hydrator.plugin.batch.source.FileBatchSource;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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

  public static File folder;
  public static File file;
  public static int port;

  private static FakeFtpServer ftpServer;

  @Rule
  public final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

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

    Assert.assertTrue(ftpServer.isStarted());
    port = ftpServer.getServerControlPort();
  }

  @After
  public void stop() throws Exception {
    if (ftpServer != null) {
      ftpServer.stop();
    }
  }

  @Test
  public void testFTPSink() throws Exception {
    String filePath = "file:///tmp/test/text.txt";
    String testData = "String for testing purposes.";

    Path textFile = new Path(filePath);
    Configuration conf = new Configuration();
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
    FSDataOutputStream writeData = fs.create(textFile);
    writeData.write(testData.getBytes());
    writeData.flush();
    writeData.close();

    ETLStage source = new ETLStage("source", new Plugin("File", ImmutableMap.<String, String>builder()
    .put(Properties.File.FILESYSTEM, "Text").put(Properties.File.PATH, filePath).build()));

    ETLStage sink = new ETLStage("sink", new Plugin("FTP", ImmutableMap.<String, String>builder()
      .put("basePath", "ftp://tom:tom@fileserver6986-1000.dev.continuuity.net:/data2").build()));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "FileToFTP");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(2, TimeUnit.MINUTES);
  }

  @Ignore
  @Test
  public void testFTPBatchSource() throws Exception {
    ETLStage source = new ETLStage("source", new Plugin("FTP", ImmutableMap.<String, String>builder()
      .put(Properties.File.PATH, String.format("ftp://%s:%s@localhost:%d%s", USER, PWD, port, folder.getAbsolutePath()))
      .build()));
    ETLStage sink = new ETLStage("sink", new Plugin("TPFSAvro", ImmutableMap.<String, String>builder()
      .put(Properties.TimePartitionedFileSetDataset.SCHEMA, FileBatchSource.DEFAULT_SCHEMA.toString())
      .put(Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink").build()));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSink(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "FTPToTPFS");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(2, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("fileSink");
    try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
      List<GenericRecord> records = readOutput(fileSet, FileBatchSource.DEFAULT_SCHEMA);
      Assert.assertEquals(1, records.size());
      GenericRecord record = records.get(0);
      Assert.assertEquals(TEST_STRING, record.get("body").toString());
    }
  }
}
