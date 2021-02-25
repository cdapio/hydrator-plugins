/*
 * Copyright © 2021 Cask Data, Inc.
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


import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * Unit test for {@link FTPBatchSource.FTPBatchSourceConfig} class.
 */

public class FTPBatchSourceTest {

  private static final String USER = "user";
  private static final String PASSWORD_WITH_SPECIAL_CHARACTERS = "wex^Yz@123#456!";
  private static final String PASSWORD_WITHOUT_SPECIAL_CHARACTERS = "wexYz123456";
  private static final String HOST = "192.168.0.179";
  private static final int FTP_DEFAULT_PORT = 21;
  private static final int SFTP_DEFAULT_PORT = 22;
  private static final String PATH = "/user-look-here.txt";
  private static final String FTP_PREFIX = "ftp";
  private static final String SFTP_PREFIX = "sftp";
  private static final String SFTP_FS_CLASS = "org.apache.hadoop.fs.sftp.SFTPFileSystem";
  private static final String FS_SFTP_IMPL = "fs.sftp.impl";

  @Test
  public void testFTPPathWithSpecialCharactersInAuth() {
    FailureCollector collector = new MockFailureCollector();
    FTPBatchSource.FTPBatchSourceConfig config = new FTPBatchSource.FTPBatchSourceConfig();
    config.configuration(String.format("%s://%s:%s@%s:%d%s", FTP_PREFIX, USER,
                                       PASSWORD_WITH_SPECIAL_CHARACTERS, HOST, FTP_DEFAULT_PORT, PATH), null);
    config.validate(collector);

    final HashMap<String, String> fileSystemProperties = new HashMap<>();
    fileSystemProperties.put("fs.ftp.host", HOST);
    fileSystemProperties.put(String.format("fs.ftp.user.%s", HOST), USER);
    fileSystemProperties.put(String.format("fs.ftp.password.%s", HOST), PASSWORD_WITH_SPECIAL_CHARACTERS);
    fileSystemProperties.put("fs.ftp.host.port", String.valueOf(FTP_DEFAULT_PORT));
    Assert.assertEquals(fileSystemProperties, config.getFileSystemProperties(collector));
  }

  @Test
  public void testFTPPathWithoutSpecialCharactersInAuth() {
    FailureCollector collector = new MockFailureCollector();
    FTPBatchSource.FTPBatchSourceConfig config = new FTPBatchSource.FTPBatchSourceConfig();
    config.configuration(String.format("%s://%s:%s@%s:%d%s", FTP_PREFIX, USER, PASSWORD_WITHOUT_SPECIAL_CHARACTERS,
                                       HOST, FTP_DEFAULT_PORT, PATH), null);
    config.validate(collector);
    Assert.assertEquals(config.getPathFromConfig(), config.getPath());
    Assert.assertEquals(0, config.getFileSystemProperties(collector).size());
  }

  @Test
  public void testMissingAuthInPath() {
    FailureCollector collector = new MockFailureCollector();
    FTPBatchSource.FTPBatchSourceConfig config = new FTPBatchSource.FTPBatchSourceConfig();
    config.configuration(String.format("%s://%s:%d%s", FTP_PREFIX, HOST, FTP_DEFAULT_PORT, PATH), null);
    try {
      config.validate(collector);
      Assert.fail();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(String.format("Missing authentication in url: %s.", config.getPathFromConfig()),
                          e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testSFTPPathWithSpecialCharactersInAuth() {
    FailureCollector collector = new MockFailureCollector();
    FTPBatchSource.FTPBatchSourceConfig config = new FTPBatchSource.FTPBatchSourceConfig();
    final HashMap<String, String> fileSystemProperties = new HashMap<>();
    config.configuration(String.format("%s://%s:%s@%s:%d%s", SFTP_PREFIX, USER, PASSWORD_WITH_SPECIAL_CHARACTERS,
                                       HOST, SFTP_DEFAULT_PORT, PATH), null);
    config.validate(collector);

    fileSystemProperties.put("fs.sftp.host", HOST);
    fileSystemProperties.put(String.format("fs.sftp.user.%s", HOST), USER);
    fileSystemProperties.put(String.format("fs.sftp.password.%s.%s", HOST, USER), PASSWORD_WITH_SPECIAL_CHARACTERS);
    fileSystemProperties.put("fs.sftp.host.port", String.valueOf(SFTP_DEFAULT_PORT));
    Assert.assertEquals(fileSystemProperties, config.getFileSystemProperties(collector));
  }

  @Test
  public void testSFTPPathWithoutSpecialCharactersInAuth() {
    FailureCollector collector = new MockFailureCollector();
    FTPBatchSource.FTPBatchSourceConfig config = new FTPBatchSource.FTPBatchSourceConfig();
    config.configuration(String.format("%s://%s:%s@%s:%d%s", SFTP_PREFIX, USER, PASSWORD_WITHOUT_SPECIAL_CHARACTERS,
                                       HOST, SFTP_DEFAULT_PORT, PATH), null);
    config.validate(collector);
    Assert.assertEquals(config.getPathFromConfig(), config.getPath());
    Assert.assertEquals(0, config.getFileSystemProperties(collector).size());
  }

  @Test
  public void testFTPWithFileSystemProperties() {
    FailureCollector collector = new MockFailureCollector();
    FTPBatchSource.FTPBatchSourceConfig config = new FTPBatchSource.FTPBatchSourceConfig();
    config.configuration(String.format("%s://%s:%s@%s:%d%s", FTP_PREFIX, USER, PASSWORD_WITHOUT_SPECIAL_CHARACTERS,
                                       HOST, FTP_DEFAULT_PORT, PATH),
                         "{\"fs.sftp.impl\": \"org.apache.hadoop.fs.sftp.SFTPFileSystem\"}");
    config.validate(collector);
    Assert.assertEquals(config.getPathFromConfig(), config.getPath());
    Assert.assertEquals(1, config.getFileSystemProperties(collector).size());
    Assert.assertEquals(SFTP_FS_CLASS,
                        config.getFileSystemProperties(collector).get(FS_SFTP_IMPL));
  }

  @Test
  public void testFTPPathWithSystemPropertiesAndSpecialCharactersInAuth() {
    FailureCollector collector = new MockFailureCollector();
    FTPBatchSource.FTPBatchSourceConfig config = new FTPBatchSource.FTPBatchSourceConfig();
    final HashMap<String, String> fileSystemProperties = new HashMap<>();
    config.configuration(String.format("%s://%s:%s@%s:%d%s", FTP_PREFIX, USER, PASSWORD_WITH_SPECIAL_CHARACTERS,
                                       HOST, FTP_DEFAULT_PORT, PATH),
                         "{\"fs.sftp.impl\": \"org.apache.hadoop.fs.sftp.SFTPFileSystem\"}");
    config.validate(collector);

    fileSystemProperties.put("fs.ftp.host", HOST);
    fileSystemProperties.put(String.format("fs.ftp.user.%s", HOST), USER);
    fileSystemProperties.put(String.format("fs.ftp.password.%s", HOST), PASSWORD_WITH_SPECIAL_CHARACTERS);
    fileSystemProperties.put("fs.ftp.host.port", String.valueOf(FTP_DEFAULT_PORT));
    fileSystemProperties.put(FS_SFTP_IMPL, SFTP_FS_CLASS);
    Assert.assertEquals(5, config.getFileSystemProperties(collector).size());
    Assert.assertEquals(fileSystemProperties, config.getFileSystemProperties(collector));
  }

  @Test
  public void testSFTPPathWithSystemPropertiesAndSpecialCharactersInAuth() {
    FailureCollector collector = new MockFailureCollector();
    FTPBatchSource.FTPBatchSourceConfig config = new FTPBatchSource.FTPBatchSourceConfig();
    final HashMap<String, String> fileSystemProperties = new HashMap<>();
    config.configuration(String.format("%s://%s:%s@%s:%d%s", SFTP_PREFIX, USER, PASSWORD_WITH_SPECIAL_CHARACTERS,
                                       HOST, SFTP_DEFAULT_PORT, PATH),
                         "{\"fs.sftp.impl\": \"org.apache.hadoop.fs.sftp.SFTPFileSystem\"}");
    config.validate(collector);

    fileSystemProperties.put("fs.sftp.host", HOST);
    fileSystemProperties.put(String.format("fs.sftp.user.%s", HOST), USER);
    fileSystemProperties.put(String.format("fs.sftp.password.%s.%s", HOST, USER), PASSWORD_WITH_SPECIAL_CHARACTERS);
    fileSystemProperties.put("fs.sftp.host.port", String.valueOf(SFTP_DEFAULT_PORT));
    fileSystemProperties.put(FS_SFTP_IMPL, SFTP_FS_CLASS);
    Assert.assertEquals(5, config.getFileSystemProperties(collector).size());
    Assert.assertEquals(fileSystemProperties, config.getFileSystemProperties(collector));
  }
}
