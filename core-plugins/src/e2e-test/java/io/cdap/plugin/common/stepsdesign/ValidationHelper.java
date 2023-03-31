/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *  Helper class used in data validation.
 */
public class ValidationHelper {
  private static String projectId = PluginPropertyUtils.pluginProp("projectId");
  private static String expectedOutputFilesBucketName = PluginPropertyUtils.pluginProp(
    "expectedOutputFilesBucketName");

  public static void listBucketObjects(String bucketName, String expectedOutputFilePath) {
    List<String> bucketObjectNames = new ArrayList<>();
    List<String> fileData = new ArrayList<>();

    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    Page<Blob> blobs = storage.list(bucketName);

    // Adding all the Objects which have data in a list.
    List<Blob> bucketObjects = StreamSupport.stream(blobs.iterateAll().spliterator(), true)
      .filter(blob -> blob.getSize() != 0)
      .collect(Collectors.toList());

    Stream<String> objectNamesWithData = bucketObjects.stream().map(blob -> blob.getName());
    objectNamesWithData.forEach(objectName -> bucketObjectNames.add(objectName));

    // Fetching the data for all the part-r Files
    for (String objectName : bucketObjectNames) {
      if (objectName.contains("part-r")) {
        Collections.addAll(fileData, fetchObjectData(projectId, bucketName, objectName));
      }
    }
     validateBothFilesData(fileData, expectedOutputFilePath);
  }

  private static String[] fetchObjectData(String projectId, String bucketName, String objectName) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    byte[] objectData = storage.readAllBytes(bucketName, objectName);
    String objectDataAsString = new String(objectData, StandardCharsets.UTF_8);

    // Splitting using the delimiter as a File can have more than one record.
    return objectDataAsString.split("\n");
  }

  private static void validateBothFilesData(List<String> outputFileData, String expectedOutputFilePath) {
    String[] expectedOutputFileData = fetchObjectData(projectId, expectedOutputFilesBucketName, expectedOutputFilePath);
    int counter = 0;

    for (String line : expectedOutputFileData) {
      Assert.assertTrue("Output file and Expected file data should be same", outputFileData.contains(line));
      counter++;
    }
    Assert.assertTrue(counter == expectedOutputFileData.length);
  }

  // TODO : Remove the methods used in data validation of local files once the bug is resolved.
  // Bug : https://cdap.atlassian.net/browse/CDAP-20503
  public static void compareData(String fileSinkBucketPath, String expectedOutputFilePath) {
    List<String> outputFileData = readAllFilesFromDirectory(PluginPropertyUtils.pluginProp(fileSinkBucketPath));
    validateBothFilesData(outputFileData, expectedOutputFilePath);
  }

  // Reading data from all part-r files present in a directory.
  public static List<String> readAllFilesFromDirectory(String directoryPath) {
    List<String> outputFileData = new ArrayList<>();
    try {
      Path dirPath = Paths.get(directoryPath);
      Files.walk(dirPath).filter(Files::isRegularFile).filter(file -> file.toFile().getName().startsWith("part-r"))
        .forEach(file -> {
          try {
            List<String> fileLines = Files.readAllLines(file);
            outputFileData.addAll(fileLines);
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
    } catch (IOException e) {
      e.printStackTrace();
    }
    return outputFileData;
  }

  // Validating the data of output Orc file and expected file.
  public static void compareDataOfOrcFiles(String expectedOutputFilePath)
    throws IOException {
    Optional<Path> sinkOutputFile =  Files.walk(Paths.get(PluginPropertyUtils.pluginProp(
      "fileSinkTargetBucket")))
      .filter(Files::isRegularFile).filter(file -> file.toFile().getName().startsWith("part-r")).findFirst();
    if (sinkOutputFile.isPresent()) {
      try {
        boolean result = compareFiles(sinkOutputFile.get().toFile().getAbsolutePath(), expectedOutputFilePath);
        Assert.assertTrue("Records in output file generated by file sink plugin should be equal to" +
                            " records in expected output file", result);
      } catch (NoSuchFileException e) {
        Assert.fail("Expected output file to compare is not present " + e);
      }
    } else {
      Assert.fail("Output part-r file is not generated by file sink plugin");
    }
  }

  private static boolean compareFiles(String firstFilePath, String secondFilePath) throws IOException {
    byte[] first = Files.readAllBytes(new File(firstFilePath).toPath());
    // fetching the data of expected output file from gcs bucket.
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    byte[] second = storage.readAllBytes(expectedOutputFilesBucketName, secondFilePath);

    return Arrays.equals(first, second);
  }
}
