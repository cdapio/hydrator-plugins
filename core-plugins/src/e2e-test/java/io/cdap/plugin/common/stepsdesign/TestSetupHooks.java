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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Common test setup hooks.
 */
public class TestSetupHooks {

  public static String gcsSourceBucketName1 = StringUtils.EMPTY;
  public static String gcsSourceBucketName2 = StringUtils.EMPTY;
  public static String gcsTargetBucketName = StringUtils.EMPTY;
  public static String fileSourceBucket1 = StringUtils.EMPTY;
  public static String fileSourceBucket2 = StringUtils.EMPTY;
  public static String fileSourceBucket3 = StringUtils.EMPTY;
  public static String fileSourceBucket4 = StringUtils.EMPTY;
  public static String fileSourceBucket5 = StringUtils.EMPTY;
  public static String fileSourceBucket6 = StringUtils.EMPTY;
  public static String fileSourceBucket7 = StringUtils.EMPTY;
  public static String fileSourceBucket8 = StringUtils.EMPTY;
  public static String fileSourceBucket9 = StringUtils.EMPTY;
  public static String fileSourceBucket10 = StringUtils.EMPTY;

  public static String testOnCdap = PluginPropertyUtils.pluginProp("testOnCdap");
  private static boolean firstFileSinkTestFlag = true;
  private static String fileSinkOutputFolder = StringUtils.EMPTY;


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

  @Before(order = 1, value = "@GCS_DEDUPLICATE_TEST")
  public static void createBucketWithDeduplicateTestFile() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("deduplicateFileCsvFile"));
    PluginPropertyUtils.addPluginProp("gcsDeduplicateTest", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("deduplicateFileCsvFile"));
    BeforeActions.scenario.write("Deduplicate bucket name - " + gcsSourceBucketName1);
  }

  @After(order = 1, value = "@GCS_DEDUPLICATE_TEST")
  public static void deleteSourceBucketWithDeduplicateTestFile() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@GCS_DISTINCT_TEST1")
  public static void createBucketWithDistinctTest1File() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("distinctCsvAllDataTypeFile"));
    PluginPropertyUtils.addPluginProp("gcsDistinctTest1", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("distinctCsvAllDataTypeFile"));
    BeforeActions.scenario.write("Distinct 1st bucket name - " + gcsSourceBucketName1);
  }

  @After(order = 1, value = "@GCS_DISTINCT_TEST1")
  public static void deleteSourceBucketWithDistinctTest1File() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@GCS_DISTINCT_TEST2")
  public static void createBucketWithDistinctTest2File() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("distinctFileCsvFile"));
    PluginPropertyUtils.addPluginProp("gcsDistinctTest2", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("distinctFileCsvFile"));
    BeforeActions.scenario.write("Distinct 2nd bucket name - " + gcsSourceBucketName1);
  }

  @After(order = 1, value = "@GCS_DISTINCT_TEST2")
  public static void deleteSourceBucketWithDistinctTest2File() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@ERROR_COLLECTOR_TEST")
  public static void createBucketWithErrorCollectorTest1File() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("csvFile"));
    PluginPropertyUtils.addPluginProp("errorCollector1", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("csvFile"));
    BeforeActions.scenario.write("Error Collector bucket name - " + gcsSourceBucketName1);
  }

  @After(order = 1, value = "@ERROR_COLLECTOR_TEST")
  public static void deleteSourceBucketWithErrorCollectorTest1File() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@GROUP_BY_TEST")
  public static void createBucketWithGroupByTest1File() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("groupByGcsCsvFile"));
    PluginPropertyUtils.addPluginProp("groupByTest", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("groupByGcsCsvFile"));
    BeforeActions.scenario.write("Group by bucket name - " + gcsSourceBucketName1);
  }

  @After(order = 1, value = "@GROUP_BY_TEST")
  public static void deleteSourceBucketWithGroupByTest1File() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@JOINER_TEST2")
  public static void createBucketWithJoinerTest2File() throws IOException, URISyntaxException {
    gcsSourceBucketName2 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp(
      "joinerCsvFileSecondInput"));
    PluginPropertyUtils.addPluginProp("joinerInputTest2", "gs://" + gcsSourceBucketName2 + "/"  +
      PluginPropertyUtils.pluginProp("joinerCsvFileSecondInput"));
    BeforeActions.scenario.write("Joiner 2nd bucket name - " + gcsSourceBucketName2);
  }

  @After(order = 1, value = "@JOINER_TEST2")
  public static void deleteSourceBucketWithJoinerTest2File() {
    deleteGCSBucket(gcsSourceBucketName2);
    gcsSourceBucketName2 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@JOINER_TEST1")
  public static void createBucketWithJoinerTest1File() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp(
      "joinerCsvFileFirstInput"));
    PluginPropertyUtils.addPluginProp("joinerInputTest1", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("joinerCsvFileFirstInput"));
    BeforeActions.scenario.write("Joiner 1st bucket name - " + gcsSourceBucketName1);
  }

  @After(order = 1, value = "@JOINER_TEST1")
  public static void deleteSourceBucketWithJoinerTest1File() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@JOINER_TEST3")
  public static void createBucketWithJoinerTest3File() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp(
      "joinerCsvNullFileFirstInput"));
    PluginPropertyUtils.addPluginProp("joinerCsvNullFileInputTest1",
     "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("joinerCsvNullFileFirstInput"));
    BeforeActions.scenario.write("Joiner 3rd bucket name - " + gcsSourceBucketName1);
  }

  @After(order = 1, value = "@JOINER_TEST3")
  public static void deleteSourceBucketWithJoinerTest3File() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@JOINER_TEST4")
  public static void createBucketWithJoinerTest4File() throws IOException, URISyntaxException {
    gcsSourceBucketName2 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp(
      "joinerCsvNullFileSecondInput"));
    PluginPropertyUtils.addPluginProp("joinerCsvNullFileInputTest2",
      "gs://" + gcsSourceBucketName2 + "/"  +
      PluginPropertyUtils.pluginProp("joinerCsvNullFileSecondInput"));
    BeforeActions.scenario.write("Joiner 4th bucket name - " + gcsSourceBucketName2);
  }

  @After(order = 1, value = "@JOINER_TEST4")
  public static void deleteSourceBucketWithJoinerTest4File() {
    deleteGCSBucket(gcsSourceBucketName2);
    gcsSourceBucketName2 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@NORMALIZE_TEST1")
  public static void createBucketWithNormalizeTest1File() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp(
      "normalizeCsvAllDataTypeFile"));
    PluginPropertyUtils.addPluginProp("normalizeTest1", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("normalizeCsvAllDataTypeFile"));
    BeforeActions.scenario.write("Normalize 1st bucket name - " + gcsSourceBucketName1);
  }

  @After(order = 1, value = "@NORMALIZE_TEST1")
  public static void deleteSourceBucketWithNormalizeTest1File() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@NORMALIZE_TEST2")
  public static void createBucketWithNormalizeTest2File() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("normalizeCsvFile"));
    PluginPropertyUtils.addPluginProp("normalizeTest2", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("normalizeCsvFile"));
    BeforeActions.scenario.write("Normalize 2nd bucket name - " + gcsSourceBucketName1);
  }

  @After(order = 1, value = "@NORMALIZE_TEST2")
  public static void deleteSourceBucketWithNormalizeTest2File() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@CSV_TEST")
  public static void createBucketWithFileCSVTest() throws IOException, URISyntaxException {
    gcsSourceBucketName1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("csvFile"));
    PluginPropertyUtils.addPluginProp("csvTest", "gs://" + gcsSourceBucketName1 + "/"  +
      PluginPropertyUtils.pluginProp("csvFile"));
    BeforeActions.scenario.write("CSV Test bucket name - " + gcsSourceBucketName1);
  }

  @After(order = 1, value = "@CSV_TEST")
  public static void deleteSourceBucketWithFileCSVTest() {
    deleteGCSBucket(gcsSourceBucketName1);
    gcsSourceBucketName1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@CSV_DATATYPE_TEST1")
  public static void createBucketWithFileCSVDataTypeTest1() throws IOException, URISyntaxException {
    fileSourceBucket1 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("csvAllDataTypeFile"));
    PluginPropertyUtils.addPluginProp("csvAllDataTypeTestFile", "gs://" + fileSourceBucket1 + "/"  +
      PluginPropertyUtils.pluginProp("csvAllDataTypeFile"));
    BeforeActions.scenario.write("CSV Datatype test bucket name - " + fileSourceBucket1);
  }

  @After(order = 1, value = "@CSV_DATATYPE_TEST1")
  public static void deleteSourceBucketWithFileCSVDataTypeTest1() {
    deleteGCSBucket(fileSourceBucket1);
    fileSourceBucket1 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@CSV_NO_HEADER_FILE")
  public static void createBucketWithCSVNoHeaderFile() throws IOException, URISyntaxException {
    fileSourceBucket2 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("csvNoHeaderFile"));
    PluginPropertyUtils.addPluginProp("csvNoHeaderTestFile", "gs://" + fileSourceBucket2 + "/"  +
      PluginPropertyUtils.pluginProp("csvNoHeaderFile"));
    BeforeActions.scenario.write("CSV No Header bucket name - " + fileSourceBucket2);
  }

  @After(order = 1, value = "@CSV_NO_HEADER_FILE")
  public static void deleteSourceBucketWithCSVNoHeaderFile() {
    deleteGCSBucket(fileSourceBucket2);
    fileSourceBucket2 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@TSV_FILE")
  public static void createBucketWithTSVFile() throws IOException, URISyntaxException {
    fileSourceBucket3 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("tsvFile"));
    PluginPropertyUtils.addPluginProp("tsvTestFile", "gs://" + fileSourceBucket3 + "/"  +
      PluginPropertyUtils.pluginProp("tsvFile"));
    BeforeActions.scenario.write("TSV File bucket name - " + fileSourceBucket3);
  }

  @After(order = 1, value = "@TSV_FILE")
  public static void deleteSourceBucketWithTSVFile() {
    deleteGCSBucket(fileSourceBucket3);
    fileSourceBucket3 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@TEXT_FILE")
  public static void createBucketWithTextFile() throws IOException, URISyntaxException {
    fileSourceBucket4 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("textFile"));
    PluginPropertyUtils.addPluginProp("textTestFile", "gs://" + fileSourceBucket4 + "/"  +
      PluginPropertyUtils.pluginProp("textFile"));
    BeforeActions.scenario.write("Text File bucket name - " + fileSourceBucket4);
  }

  @After(order = 1, value = "@TEXT_FILE")
  public static void deleteSourceBucketWithTextFile() {
    deleteGCSBucket(fileSourceBucket4);
    fileSourceBucket4 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@PATH_FIELD_FILE")
  public static void createBucketWithPathFieldFile() throws IOException, URISyntaxException {
    fileSourceBucket5 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("pathFieldTestFile"));
    PluginPropertyUtils.addPluginProp("pathFieldTestFiles", "gs://" + fileSourceBucket5 + "/"  +
      PluginPropertyUtils.pluginProp("pathFieldTestFile"));
    BeforeActions.scenario.write("Path Field File bucket name - " + fileSourceBucket5);
  }

  @After(order = 1, value = "@PATH_FIELD_FILE")
  public static void deleteSourceBucketWithPathFieldFile() {
    deleteGCSBucket(fileSourceBucket5);
    fileSourceBucket5 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@DELIMITED_FILE")
  public static void createBucketWithDelimitedFile() throws IOException, URISyntaxException {
    fileSourceBucket6 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("delimitedFile"));
    PluginPropertyUtils.addPluginProp("delimitedTestFile", "gs://" + fileSourceBucket6 + "/"  +
      PluginPropertyUtils.pluginProp("delimitedFile"));
    BeforeActions.scenario.write("Delimited File bucket name - " + fileSourceBucket6);
  }

  @After(order = 1, value = "@DELIMITED_FILE")
  public static void deleteSourceBucketWithDelimitedFile() {
    deleteGCSBucket(fileSourceBucket6);
    fileSourceBucket6 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@QUOTED_VALUE_CSV")
  public static void createBucketWithQuotedValueCsvFile() throws IOException, URISyntaxException {
    fileSourceBucket7 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("quotedValueCSVFile"));
    PluginPropertyUtils.addPluginProp("quotedValueCSVTestFiles", "gs://" + fileSourceBucket7 + "/"  +
      PluginPropertyUtils.pluginProp("quotedValueCSVFile"));
    BeforeActions.scenario.write("Quoted Value CSV File bucket name - " + fileSourceBucket7);
  }

  @After(order = 1, value = "@QUOTED_VALUE_CSV")
  public static void deleteSourceBucketWithQuotedValueCsvFile() {
    deleteGCSBucket(fileSourceBucket7);
    fileSourceBucket7 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@QUOTED_VALUE_TSV")
  public static void createBucketWithQuotedValueTsvFile() throws IOException, URISyntaxException {
    fileSourceBucket8 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("quotedValueTSVFile"));
    PluginPropertyUtils.addPluginProp("quotedValueTSVTestFiles", "gs://" + fileSourceBucket8 + "/"  +
      PluginPropertyUtils.pluginProp("quotedValueTSVFile"));
    BeforeActions.scenario.write("Quoted Value TSV File bucket name - " + fileSourceBucket8);
  }

  @After(order = 1, value = "@QUOTED_VALUE_TSV")
  public static void deleteSourceBucketWithQuotedValueTsvFile() {
    deleteGCSBucket(fileSourceBucket8);
    fileSourceBucket8 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@QUOTED_VALUE_TXT")
  public static void createBucketWithQuotedValueTxtFile() throws IOException, URISyntaxException {
    fileSourceBucket9 = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("quotedValueDelimitedFile"));
    PluginPropertyUtils.addPluginProp("quotedValueDelimitedTestFiles", "gs://" + fileSourceBucket9 + "/"  +
      PluginPropertyUtils.pluginProp("quotedValueDelimitedFile"));
    BeforeActions.scenario.write("Quoted Value Text File bucket name - " + fileSourceBucket9);
  }

  @After(order = 1, value = "@QUOTED_VALUE_TXT")
  public static void deleteSourceBucketWithQuotedValueTxtFile() {
    deleteGCSBucket(fileSourceBucket9);
    fileSourceBucket9 = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@FILE_SINK_TEST")
  public static void setTempTargetFileSinkBucketName() throws IOException {
    if (testOnCdap.equals("true")) {
      if (firstFileSinkTestFlag) {
        fileSinkOutputFolder = PluginPropertyUtils.pluginProp("fileSinkTargetBucket");
        firstFileSinkTestFlag = false;
      }

      PluginPropertyUtils.addPluginProp("fileSinkTargetBucket", Paths.get("target/"
        + fileSinkOutputFolder + "/" + (new SimpleDateFormat("yyyyMMdd-HH-mm-ssSSS").format(
        new Date()))).toAbsolutePath().toString());
    } else {
      gcsTargetBucketName = createGCSBucket();
      PluginPropertyUtils.addPluginProp("fileSinkTargetBucket", "gs://" + gcsTargetBucketName);
      BeforeActions.scenario.write("File Sink target bucket name - " + gcsTargetBucketName);
    }
  }

  @After(order = 1, value = "@FILE_SINK_TEST")
  public static void deleteTargetFileSinkBucketWithFile() throws IOException {
    if (testOnCdap.equals("true")) {
      FileUtils.deleteDirectory(new File(PluginPropertyUtils.pluginProp("fileSinkTargetBucket")));
    } else {
      deleteGCSBucket(gcsTargetBucketName);
      gcsTargetBucketName = StringUtils.EMPTY;
    }
  }

  @Before(order = 1, value = "@RECURSIVE_TEST")
  public static void setTempTargetBucketWithRecursiveFile() throws IOException, URISyntaxException {
    fileSourceBucket10 = createGCSBucketWithMultipleFiles(PluginPropertyUtils.pluginProp("readRecursivePath"));
    PluginPropertyUtils.addPluginProp("recursiveTest", "gs://" + fileSourceBucket10 + "/" +
      PluginPropertyUtils.pluginProp("readRecursivePath"));
    BeforeActions.scenario.write("Target bucket name - " + fileSourceBucket10);
  }

  @After(order = 1, value = "@RECURSIVE_TEST")
  public static void deleteTempTargetBucketWithRecursiveFile() {
    deleteGCSBucket(fileSourceBucket10);
    fileSourceBucket10 = StringUtils.EMPTY;
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

  public static String createGCSBucketWithMultipleFiles(String folderPath) throws IOException, URISyntaxException {
    List<File> files = Files.list(Paths.get(StorageClient.class.getResource("/" + folderPath).toURI()))
      .filter(Files::isRegularFile)
      .map(Path::toFile)
      .collect(Collectors.toList());

    String bucketName = StorageClient.createBucket("cdf-e2e-test-" + UUID.randomUUID()).getName();
    for (File file : files) {
      String filePath = folderPath + "/" + file.getName();
      StorageClient.uploadObject(bucketName, filePath, filePath);
    }
    BeforeActions.scenario.write("Created GCS Bucket " + bucketName + " containing "
                                   + files.size() + " files in " + folderPath);
    return bucketName;
  }
}
