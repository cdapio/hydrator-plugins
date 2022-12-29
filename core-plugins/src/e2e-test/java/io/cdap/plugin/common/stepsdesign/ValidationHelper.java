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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    // Fetching the data for all the part-r Files.
    for (String objectName : bucketObjectNames) {
      if (objectName.contains("part-r")) {
        Collections.addAll(fileData, fetchObjectData(projectId, bucketName, objectName));
      }
    }
     validateBothFilesData(fileData, expectedOutputFilePath);
  }

  public static String[] fetchObjectData(String projectId, String bucketName, String objectName) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    byte[] objectData = storage.readAllBytes(bucketName, objectName);
    String objectDataAsString = new String(objectData, StandardCharsets.UTF_8);

    // Splitting using the delimiter as a File can have more than one record.
    return objectDataAsString.split("\n");
  }

  public static void validateBothFilesData(List<String> outputFileData, String expectedOutputFilePath) {
    String[] expectedOutputFileData = fetchObjectData(projectId, expectedOutputFilesBucketName, expectedOutputFilePath);
    int counter = 0;

    for (String line : expectedOutputFileData) {
      if (outputFileData.contains(line)) {
        counter++;
      }
    }
    Assert.assertTrue(counter == expectedOutputFileData.length);
  }
}
