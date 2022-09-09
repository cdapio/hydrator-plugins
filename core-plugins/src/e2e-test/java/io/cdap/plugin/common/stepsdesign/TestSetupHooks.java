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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * Common test setup hooks.
 */
public class TestSetupHooks {

  private static boolean firstFileSourceTestFlag = true;
  private static boolean firstFileSinkTestFlag = true;
  private static String fileSinkOutputFolder = StringUtils.EMPTY;
  public static String gcsSourceBucketName1 = StringUtils.EMPTY;
  public static String gcsSourceBucketName2 = StringUtils.EMPTY;
  public static String gcsTargetBucketName = StringUtils.EMPTY;

  @Before(order = 1, value = "@FILE_SOURCE_TEST")
  public static void setFileSourceAbsolutePath() {
    if (firstFileSourceTestFlag) {
      PluginPropertyUtils.addPluginProp("csvFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("csvFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("csvNoHeaderFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("csvNoHeaderFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("csvAllDataTypeFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("csvAllDataTypeFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("tsvFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("tsvFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("blobFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("blobFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("delimitedFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("delimitedFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("textFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("textFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputFieldTestFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputFieldTestFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("readRecursivePath", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("readRecursivePath")).getPath()) + "/");
      PluginPropertyUtils.addPluginProp("normalizeCsvFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("normalizeCsvFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("normalizeCsvAllDataTypeFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("normalizeCsvAllDataTypeFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("distinctCsvAllDataTypeFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("distinctCsvAllDataTypeFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("distinctFileCsvFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("distinctFileCsvFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("deduplicateFileCsvFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("deduplicateFileCsvFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("groupByGcsCsvFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("groupByGcsCsvFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("joinerCsvFileSecondInput", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("joinerCsvFileSecondInput")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("joinerCsvFileFirstInput", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("joinerCsvFileFirstInput")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("joinerCsvNullFileSecondInput", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("joinerCsvNullFileSecondInput")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("joinerCsvNullFileFirstInput", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("joinerCsvNullFileFirstInput")).getPath()).toString());
      firstFileSourceTestFlag = false;
    }
  }

  @Before(order = 1, value = "@FILE_SINK_TEST")
  public static void setFileSinkAbsolutePath() {
    if (firstFileSinkTestFlag) {
      PluginPropertyUtils.addPluginProp("normalizeCsvAllDataTypeOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("normalizeCsvAllDataTypeOutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("normalizeCsvOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("normalizeCsvOutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("distinctDatatypeOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("distinctDatatypeOutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("distinctCsvOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("distinctCsvOutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("distinctMacroOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("distinctMacroOutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("deduplicateTest1OutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("deduplicateTest1OutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("deduplicateTest2OutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("deduplicateTest2OutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("deduplicateTest3OutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("deduplicateTest3OutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("deduplicateMacroOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("deduplicateMacroOutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("groupByTest1OutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("groupByTest1OutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("groupByTest2OutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("groupByTest2OutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("groupByTest3OutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("groupByTest3OutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("groupByMacroOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("groupByMacroOutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("joinerTest1OutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("joinerTest1OutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("joinerTest2OutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("joinerTest2OutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("joinerTest3OutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("joinerTest3OutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("joinerTest4OutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("joinerTest4OutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("joinerMacroOutputFile", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("joinerMacroOutputFile")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("errorCollectorDefaultConfigOutput", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("errorCollectorDefaultConfigOutput")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("errorCollectorCustomConfigOutput", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("errorCollectorCustomConfigOutput")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputForAllDataTypeTest", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputForAllDataTypeTest")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputForTsvInputTest", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputForTsvInputTest")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputForOutputFieldTest", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputForOutputFieldTest")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputForOverrideTest", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputForOverrideTest")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputForDelimitedTest", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputForDelimitedTest")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputForRegexTest", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputForRegexTest")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputForRecursiveTest", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputForRecursiveTest")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputForCSVInputTest", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputForCSVInputTest")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputForTextInputTest", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputForTextInputTest")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("outputForCSVDelimitedOutputTest", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("outputForCSVDelimitedOutputTest")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("tsvFormatOutput", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("tsvFormatOutput")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("jsonFormatOutput", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("jsonFormatOutput")).getPath()).toString());
      PluginPropertyUtils.addPluginProp("orcFormatOutput", Paths.get(TestSetupHooks.class.getResource
        ("/" + PluginPropertyUtils.pluginProp("orcFormatOutput")).getPath()).toString());
      fileSinkOutputFolder = PluginPropertyUtils.pluginProp("filePluginOutputFolder");
      firstFileSinkTestFlag = false;
    }
    PluginPropertyUtils.addPluginProp("filePluginOutputFolder", Paths.get("target/" + fileSinkOutputFolder + "/"
             + (new SimpleDateFormat("yyyyMMdd-HH-mm-ssSSS").format(new Date()))).toAbsolutePath().toString());
  }

  @After(order = 1, value = "@FILE_SINK_TEST")
  public static void deleteFileSinkOutputFolder() throws IOException {
    FileUtils.deleteDirectory(new File(PluginPropertyUtils.pluginProp("filePluginOutputFolder")));
  }

  @Before(order = 1, value = "@GCS_SOURCE_TEST")
  public static void createBucketWithCSVFile() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("firstNameCsvFile"));
    PluginPropertyUtils.addPluginProp("gcsSourceBucket1", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("firstNameCsvFile"));
    BeforeActions.scenario.write("GCS source bucket1 name - " + gcsSourceBucketName1);
  }

  @Before(order = 1, value = "@GCS_SOURCE_JOINER_TEST")
  public static void createBucketWithCSVFileForJoinerTest() throws IOException, URISyntaxException {
    gcsSourceBucketName2 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("lastNameCsvFile"));
    PluginPropertyUtils.addPluginProp("gcsSourceBucket2", "gs://" + gcsSourceBucketName2 + "/" +
      PluginPropertyUtils.pluginProp("lastNameCsvFile"));
    BeforeActions.scenario.write("GCS source bucket2 name - " + gcsSourceBucketName2);
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
    return StorageClient.createBucket("e2e-test-" + UUID.randomUUID()).getName();
  }

  private static String createGCSBucketWithFile(String filePath) throws IOException, URISyntaxException {
    String bucketName = StorageClient.createBucket("e2e-test-" + UUID.randomUUID()).getName();
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
}
