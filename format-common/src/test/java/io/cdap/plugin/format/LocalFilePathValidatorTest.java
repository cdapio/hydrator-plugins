/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * Unit tests for {@link LocalFilePathValidator}.
 */
public class LocalFilePathValidatorTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testAllowedRemoteAndTempPaths() throws Exception {
    Configuration conf = new Configuration();
    LocalFilePathValidator.validatePath("gs://my-bucket/data/input.csv", conf);
    LocalFilePathValidator.validatePath("hdfs://namenode:8020/user/cdap/input.csv", conf);
    LocalFilePathValidator.validatePath("s3a://bucket/dir/file.parquet", conf);

    File tempDir = TMP_FOLDER.newFolder();
    File tempFile = new File(tempDir, "valid_data.csv");
    Assert.assertTrue(tempFile.createNewFile());

    LocalFilePathValidator.validatePath(tempFile.getAbsolutePath(), conf);
    LocalFilePathValidator.validatePath(tempFile.toURI().toString(), conf);
    LocalFilePathValidator.validatePath(new File(tempDir, "*.csv").getAbsolutePath(), conf);
  }

  @Test
  public void testRestrictedSystemDirectoriesRejected() {
    Configuration conf = new Configuration();
    String[] restrictedPaths = new String[]{
        "/var/run/secrets/kubernetes.io/serviceaccount/token",
        "file:///var/run/secrets/kubernetes.io/serviceaccount/token",
        "local:///var/run/secrets/kubernetes.io/serviceaccount/token",
        "/run/secrets/kubernetes.io/serviceaccount/token",
        "/proc/self/environ",
        "file:///proc/self/cmdline",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        "/etc/passwd",
        "file:///etc/cdap/conf/cdap-site.xml",
        "/root/.ssh/id_rsa",
        "/dev/urandom",
        "/var/log/syslog",
        "/var/lib/kubelet/pods/123"
    };

    for (String restricted : restrictedPaths) {
      try {
        LocalFilePathValidator.validatePath(restricted, conf);
        Assert.fail("Expected IllegalArgumentException for restricted path: " + restricted);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }

  @Test
  public void testParentTraversalRejected() {
    Configuration conf = new Configuration();
    String[] traversalPaths = new String[]{
        "/tmp/../etc/passwd",
        "file:///tmp/subdir/../../var/run/secrets/kubernetes.io/serviceaccount/token",
        "/tmp/%2e%2e/etc/passwd",
        "relative/../../etc/shadow"
    };

    for (String path : traversalPaths) {
      try {
        LocalFilePathValidator.validatePath(path, conf);
        Assert.fail("Expected IllegalArgumentException for traversal path: " + path);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }

  @Test
  public void testJarSchemeRejected() {
    Configuration conf = new Configuration();
    try {
      LocalFilePathValidator.validatePath("jar:file:///tmp/archive.jar!/entry.txt", conf);
      Assert.fail("Expected IllegalArgumentException for jar: URI");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testSensitiveConfigAndCredentialFilesRejected() {
    Configuration conf = new Configuration();
    String[] sensitiveFiles = new String[]{
        "/tmp/work/cConf.xml",
        "/tmp/work/hConf.xml",
        "/tmp/work/cdap-site.xml",
        "/tmp/work/cdap-security.xml",
        "/tmp/work/core-site.xml",
        "/tmp/work/hdfs-site.xml",
        "/tmp/work/logback.xml",
        "/tmp/work/logback-container.xml",
        "/tmp/work/router.jks",
        "/tmp/work/server.keystore",
        "/tmp/work/client.truststore",
        "/tmp/work/cdap.keytab",
        "/tmp/work/private.pem"
    };

    for (String path : sensitiveFiles) {
      try {
        LocalFilePathValidator.validatePath(path, conf);
        Assert.fail("Expected IllegalArgumentException for sensitive file: " + path);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }

  @Test
  public void testSymlinkToRestrictedPathRejected() throws Exception {
    Configuration conf = new Configuration();
    File dir = TMP_FOLDER.newFolder();

    // 1. Dangling symlink pointing to /var/run/secrets/kubernetes.io/serviceaccount/token
    Path secretSymlink = dir.toPath().resolve("k8s_token_link.txt");
    Files.createSymbolicLink(
        secretSymlink, Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/token"));
    try {
      LocalFilePathValidator.validatePath(secretSymlink.toString(), conf);
      Assert.fail("Expected IllegalArgumentException for symlink to /var/run/secrets");
    } catch (IllegalArgumentException expected) {
      // expected
    }

    // 2. Live symlink pointing to /etc/passwd
    Path passwdSymlink = dir.toPath().resolve("passwd_link.txt");
    Files.createSymbolicLink(passwdSymlink, Paths.get("/etc/passwd"));
    try {
      LocalFilePathValidator.validatePath(passwdSymlink.toString(), conf);
      Assert.fail("Expected IllegalArgumentException for symlink to /etc/passwd");
    } catch (IllegalArgumentException expected) {
      // expected
    }

    // 3. Directory symlink pointing to /etc
    Path etcDirSymlink = dir.toPath().resolve("etc_dir");
    Files.createSymbolicLink(etcDirSymlink, Paths.get("/etc"));
    try {
      LocalFilePathValidator.validatePath(etcDirSymlink.resolve("hosts").toString(), conf);
      Assert.fail("Expected IllegalArgumentException for child inside symlinked /etc directory");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testLocalFileKillSwitchProperty() throws Exception {
    File tempFile = TMP_FOLDER.newFile("killswitch.csv");
    Configuration conf = new Configuration();
    conf.setBoolean(LocalFilePathValidator.LOCAL_FILE_DISABLED_PROPERTY, true);

    try {
      LocalFilePathValidator.validatePath(tempFile.getAbsolutePath(), conf);
      Assert.fail("Expected IllegalArgumentException when local file access is disabled");
    } catch (IllegalArgumentException expected) {
      // expected
    }

    // Remote URI is still permitted when local file access is disabled
    LocalFilePathValidator.validatePath("gs://my-bucket/input.csv", conf);
  }

  @Test
  public void testRestrictedFileSystemPropertiesRejected() {
    LocalFilePathValidator.validateFileSystemProperties(
        Collections.singletonMap("custom.property", "value"));

    String[] blockedKeys = new String[]{
        FileSystem.FS_DEFAULT_NAME_KEY,
        "fs.default.name",
        "fs.file.impl",
        "fs.AbstractFileSystem.file.impl",
        "fs.local.impl",
        "fs.AbstractFileSystem.local.impl",
        "fs.jar.impl",
        "fs.AbstractFileSystem.jar.impl",
        LocalFilePathValidator.LOCAL_FILE_DISABLED_PROPERTY
    };

    for (String key : blockedKeys) {
      try {
        LocalFilePathValidator.validateFileSystemProperties(
            Collections.singletonMap(key, "org.apache.hadoop.fs.LocalFileSystem"));
        Assert.fail("Expected IllegalArgumentException for restricted fs property: " + key);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }
}
