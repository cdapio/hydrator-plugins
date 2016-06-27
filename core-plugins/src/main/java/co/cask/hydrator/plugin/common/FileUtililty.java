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
package co.cask.hydrator.plugin.common;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * FileSystem Utility class doing various operations on file.
 */
public final class FileUtililty {

  private FileUtililty() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Method to archive source file into target
   * @param fileSystem - file system
   * @param sourceFile - source file to be archived
   * @param targetFile - target file where source file to be archived.
   * @param deleteSource - True if file to be deleted after archiving otherwise false.
   * @throws IOException - IOException occurred while archiving file.
   */
  public static void archiveFile(FileSystem fileSystem, Path sourceFile, Path targetFile, boolean deleteSource) throws
    IOException {
    try (FSDataOutputStream archivedStream = fileSystem.create(targetFile);
         ZipOutputStream zipArchivedStream = new ZipOutputStream(archivedStream);
         FSDataInputStream fdDataInputStream = fileSystem.open(sourceFile)) {
      zipArchivedStream.putNextEntry(new ZipEntry(sourceFile.getName()));
      int length;
      byte[] buffer = new byte[1024];
      while ((length = fdDataInputStream.read(buffer)) > 0) {
        zipArchivedStream.write(buffer, 0, length);
      }
      zipArchivedStream.closeEntry();
    }
    if (deleteSource) {
      fileSystem.delete(sourceFile, true);
    }
  }
}
