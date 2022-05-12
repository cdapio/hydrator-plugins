/*
 *  Copyright ©2022 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.plugin.format.input;

import io.cdap.plugin.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PathTrackingConfigTest {
  String pathStr;
  Job job;
  Configuration hconf;
  PathTrackingConfig conf = new PathTrackingConfig();

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void init() throws IOException {
    job = JobUtils.createInstance();
    hconf = job.getConfiguration();
  }
  /**
   * Check whether two list of Path are same, ignore the order.
   */
  public Boolean equalPaths(List<Path> paths1, List<Path> paths2) {
    String p1;
    String p2;
    int l1 = paths1.size();
    int l2 = paths2.size();
    if (l1 != l2) {
      return false;
    }
    for (int i = 0; i < l1; i++) {
      p1 = paths1.get(i).toString();
      p2 = paths2.get(i).toString();
      if (!p1.equals(p2)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Test read all parquet file from a directory
   */
  @Test
  public void testGetFilePathsForSchemaGeneration1() throws IOException {
    File testDir = tmpFolder.newFolder("d");
    File testFile1 = File.createTempFile("part-0", ".parquet", testDir);
    File testFile2 = File.createTempFile("part-1", ".parquet", testDir);
    pathStr = testDir.getAbsolutePath();

    // Get List of Path from getFilePathsForSchemaGeneration
    List<Path> filePaths = conf.getFilePathsForSchemaGeneration(pathStr, ".+\\.parquet$", hconf, job);

    // Created Expected List of Path
    List<Path> expectedPaths = new ArrayList<Path>();
    expectedPaths.add(new Path("file:" + testFile1.getAbsolutePath()));
    expectedPaths.add(new Path("file:" + testFile2.getAbsolutePath()));

    Collections.sort(expectedPaths);
    Collections.sort(filePaths);
    Assert.assertTrue(equalPaths(expectedPaths, filePaths));
  }

  /**
   * Test read all parquet file from a directory ending with .parquet
   */
  @Test
  public void testGetFilePathsForSchemaGeneration2() throws IOException {
    File testDir = tmpFolder.newFolder("d.parquet");
    File testFile1 = File.createTempFile("part-0", ".parquet", testDir);
    File testFile2 = File.createTempFile("part-1", ".parquet", testDir);
    File testFile3 = File.createTempFile("part-2", ".txt", testDir);
    pathStr = testDir.getAbsolutePath();

    // Get List of Path from getFilePathsForSchemaGeneration
    List<Path> filePaths = conf.getFilePathsForSchemaGeneration(pathStr, ".+\\.parquet$", hconf, job);

    // Created Expected List of Path
    List<Path> expectedPaths = new ArrayList<Path>();
    expectedPaths.add(new Path("file:" + testFile1.getAbsolutePath()));
    expectedPaths.add(new Path("file:" + testFile2.getAbsolutePath()));

    Collections.sort(expectedPaths);
    Collections.sort(filePaths);
    Assert.assertTrue(equalPaths(expectedPaths, filePaths));
  }

  /**
   * Test read all avro file from a directory
   */
  @Test
  public void testGetFilePathsForSchemaGeneration3() throws IOException {
    File testDir = tmpFolder.newFolder("d2");
    File testFile1 = File.createTempFile("avro-0", ".avro", testDir);
    File testFile2 = File.createTempFile("avro-1", ".avro", testDir);
    pathStr = testDir.getAbsolutePath();

    // Get List of Path from getFilePathsForSchemaGeneration
    List<Path> filePaths = conf.getFilePathsForSchemaGeneration(pathStr, ".+\\.avro$", hconf, job);

    // Created Expected List of Path
    List<Path> expectedPaths = new ArrayList<Path>();
    expectedPaths.add(new Path("file:" + testFile1.getAbsolutePath()));
    expectedPaths.add(new Path("file:" + testFile2.getAbsolutePath()));

    Collections.sort(expectedPaths);
    Collections.sort(filePaths);
    Assert.assertTrue(equalPaths(expectedPaths, filePaths));
  }

  /**
   * Test read all avro file from a directory ending with .avro
   */
  @Test
  public void testGetFilePathsForSchemaGeneration4() throws IOException {
    File testDir = tmpFolder.newFolder("d.avro");
    File testFile1 = File.createTempFile("avro-0", ".avro", testDir);
    File testFile2 = File.createTempFile("avro-1", ".avro", testDir);
    File testFile3 = File.createTempFile("avro-2", ".txt", testDir);
    pathStr = testDir.getAbsolutePath();

    // Get List of Path from getFilePathsForSchemaGeneration
    List<Path> filePaths = conf.getFilePathsForSchemaGeneration(pathStr, ".+\\.avro$", hconf, job);

    // Created Expected List of Path
    List<Path> expectedPaths = new ArrayList<Path>();
    expectedPaths.add(new Path("file:" + testFile1.getAbsolutePath()));
    expectedPaths.add(new Path("file:" + testFile2.getAbsolutePath()));

    Collections.sort(expectedPaths);
    Collections.sort(filePaths);
    Assert.assertTrue(equalPaths(expectedPaths, filePaths));
  }
}
