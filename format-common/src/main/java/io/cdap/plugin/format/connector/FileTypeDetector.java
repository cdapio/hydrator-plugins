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
 *
 */

package io.cdap.plugin.format.connector;

import io.cdap.plugin.format.FileFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Class to get the type of the file by extension. The logic of this class is mostly copy pasted from Wrangler's
 * FileTypeDetector.
 * This is to preserve backward compatibility.
 * TODO: Check if we can simplify the type returned or simply remove this.
 */
public class FileTypeDetector {
  private static final Logger LOG = LoggerFactory.getLogger(FileTypeDetector.class);
  private static final String RESOURCE_NAME = "/file.extensions";
  private static final Map<String, String> EXTENSIONS;
  private static final String PATH_SEPARATOR = "/";
  private static final String EXTENSION_SEPARATOR = ".";
  private static final String TRAILING_REGEX = "/+$";

  // Initialize project properties from .properties file.
  static {
    Map<String, String> extensions = new HashMap<>();
    try (Scanner scanner = new Scanner(FileTypeDetector.class.getResource(RESOURCE_NAME).openStream(), "UTF-8")) {
      while (scanner.hasNext()) {
        String line = scanner.nextLine();
        String[] parts = line.split("\\s+");
        if (parts.length == 2) {
          extensions.put(parts[0], parts[1]);
        }
      }
    } catch (Exception e) {
      LOG.warn("Unable to load extension map.", e);
    }
    EXTENSIONS = extensions;
  }

  private FileTypeDetector() {
    // no-op
  }

  /**
   * This function checks if the type is sampleable or not.
   *
   * Currently it only assumes a limited set of types as sampleable. The type should come from file.extensions
   *
   * @param type the type from file.extensions
   * @return true if it's sampleable, false otherwise.
   */
  public static boolean isSampleable(String type) {
    if ("text/plain".equalsIgnoreCase(type)
          || "application/json".equalsIgnoreCase(type)
          || "application/xml".equalsIgnoreCase(type)
          || "application/avro".equalsIgnoreCase(type)
          || "application/protobuf".equalsIgnoreCase(type)
          || "application/excel".equalsIgnoreCase(type)
          || type.contains("image/")
          || type.contains("text/")) {
      return true;
    }
    return false;
  }

  /**
   * Attempts to detect the type of the file through extensions.
   *
   * @param path of the file whose content type need to be detected.
   * @return type of the file.
   */
  public static String detectFileType(String path) {
    // remove trailing slash
    path = path.replaceAll(TRAILING_REGEX, "");
    // We first attempt to detect the type of file based on extension.
    int lastDot = path.lastIndexOf(EXTENSION_SEPARATOR);
    int lastSlash = path.lastIndexOf(PATH_SEPARATOR);
    // if last slash > last dot, that means we are getting something like /my.folder/file
    lastDot = lastSlash > lastDot ? -1 : lastDot;
    String extension = lastDot == -1 ? "" : path.substring(lastDot + 1).toLowerCase();

    if (EXTENSIONS.containsKey(extension)) {
      return EXTENSIONS.get(extension);
    }

    // next we check if the file does not have an extension
    String fileName = path.substring(lastSlash + 1);
    lastDot = fileName.lastIndexOf(EXTENSION_SEPARATOR);
    String baseName = lastDot == -1 ? fileName : fileName.substring(0, lastDot);
    if (baseName.equals(fileName)) {
      // CDAP-14397 return text type if there is no extension in the filename.
      return EXTENSIONS.get("txt");
    }
    return "unknown";
  }

  public static FileFormat detectFileFormat(String fileType) {
    // hack for file format, only has text and blob now, json and text are considered to use TEXT, others use BLOB
    if (fileType.contains("text/") || fileType.equals("application/json")) {
      return FileFormat.TEXT;
    }
    return FileFormat.BLOB;
  }
}
