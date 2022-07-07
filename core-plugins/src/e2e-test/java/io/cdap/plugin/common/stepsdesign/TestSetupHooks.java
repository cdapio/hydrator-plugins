/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.plugin.common.stepsdesign;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.StringUtils;
import org.apache.directory.api.util.Strings;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;

/**
 * Joiner test hooks.
 */
public class TestSetupHooks {

  public static String gcsSourceBucketName1 = StringUtils.EMPTY;
  public static String gcsSourceBucketName2 = StringUtils.EMPTY;
  public static String gcsTargetBucketName = StringUtils.EMPTY;

  @Before(order = 1, value = "@GCS_SOURCE_TEST")
  public static void createBucketWithCSVFile() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("firstNameCsvFile"));
    PluginPropertyUtils.addPluginProp("gcsSourceBucket1", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("firstNameCsvFile"));
  }

  @Before(order = 1, value = "@GCS_SOURCE_JOINER_TEST")
  public static void createBucketWithCSVFileForJoinerTest() throws IOException, URISyntaxException {
    gcsSourceBucketName2 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("lastNameCsvFile"));
    PluginPropertyUtils.addPluginProp("gcsSourceBucket2", "gs://" + gcsSourceBucketName2 + "/" +
      PluginPropertyUtils.pluginProp("lastNameCsvFile"));
  }

  @After(order = 1, value = "@GCS_SOURCE_TEST")
  public static void deleteSourceBucketWithFile() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @After(order = 1, value = "@GCS_SOURCE_JOINER_TEST")
  public static void deleteSourceBucketWithFileForJoinerTest() {
    deleteGCSBucket(gcsSourceBucketName2);
    gcsSourceBucketName2 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@GCS_SINK_TEST")
  public static void setTempTargetGCSBucketName() throws IOException {
    gcsTargetBucketName = createGCSBucket();
    PluginPropertyUtils.addPluginProp("gcsTargetBucket", "gs://" + gcsTargetBucketName);
    BeforeActions.scenario.write("GCS target bucket name - " + gcsTargetBucketName);
  }

  @After(order = 1, value = "@GCS_SINK_TEST")
  public static void deleteTargetBucketWithFile() {
    deleteGCSBucket(gcsTargetBucketName);
    gcsTargetBucketName = StringUtils.EMPTY;
  }

  private static String createGCSBucket() throws IOException {
    return StorageClient.createBucket("hdf-e2e-test-" + UUID.randomUUID()).getName();
  }

  private static String createGCSBucketWithFile(String filePath) throws IOException, URISyntaxException {
    String bucketName = StorageClient.createBucket("hdf-e2e-test-" + UUID.randomUUID()).getName();
    StorageClient.uploadObject(bucketName, filePath, filePath);
    return bucketName;
  }

  private static void deleteGCSBucket(String bucketName) {
    try {
      for (Blob blob : StorageClient.listObjects(bucketName).iterateAll()) {
        StorageClient.deleteObject(bucketName, blob.getName());
      }
      StorageClient.deleteBucket(bucketName);
      BeforeActions.scenario.write("Deleted GCS Bucket " + bucketName);
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        BeforeActions.scenario.write("GCS Bucket " + bucketName + " does not exist.");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  static String readFile(String path) throws IOException {
    File file = new File(path);
    FileInputStream fis = new FileInputStream(file);
    byte[] data = new byte[(int) file.length()];
    fis.read(data);
    fis.close();
    return new String(data, StandardCharsets.UTF_8);
  }

  public static void verifyOutputContentAndPartitions() {
    try {
      String path = Paths.get(Objects.requireNonNull(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("joinerOutput"))).getPath()).toString();
      String expectedOutput = readFile(path);
      int partitions = 0;
      StringBuilder sb = new StringBuilder();
      // The output gcs folder will be like:
      // hdf-e2e-test-xxxxxx
      // --2022-06-26-00-27/
      // ----_SUCCESS
      // ----part-r-0000
      // ----part-r-0001
      // ----part-r-...
      // The number of part-r-* files should match the expected partitions.
      for (Blob blob : StorageClient.listObjects(gcsTargetBucketName).iterateAll()) {
        String name = blob.getName();
        if (name.contains("part-r")) {
          partitions++;
          sb.append(new String((blob.getContent())));
        }
      }
      Assert.assertEquals("Output partition should match",
                          partitions, Integer.parseInt(PluginPropertyUtils.pluginProp("expectedPartitions")));
      Assert.assertTrue("Output content should match", Strings.equals(sb.toString(), expectedOutput));
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        BeforeActions.scenario.write("GCS Bucket " + gcsTargetBucketName + " does not exist.");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
}
