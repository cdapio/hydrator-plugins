/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.file;

import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class that contains file metadata fields.
 * Extend from this class to add credentials specific to different filesystems.
 */
public abstract class AbstractFileMetadata {
  public static final String FILE_NAME = "fileName";
  public static final String FILE_SIZE = "fileSize";
  public static final String TIMESTAMP = "timeStamp";
  public static final String OWNER = "owner";
  public static final String FULL_PATH = "fullPath";
  public static final String IS_FOLDER = "isFolder";
  public static final String BASE_PATH = "basePath";
  public static final String PERMISSION = "permission";

  // contains only the name of the file
  protected String fileName;

  // full path of the file in the source filesystem
  protected String fullPath;

  // file size
  protected long fileSize;

  // modification time of file
  protected long timeStamp;

  // owner of file
  protected String owner;

  // whether or not the file is a folder
  protected Boolean isFolder;

  // the base path that will be appended to the path the sink is writing to
  protected String basePath;

  // perission of file, encoded in short
  protected short permission;

  // Credentials needed to connect to the filesystem that contains this file
  protected final Credentials credentials;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileMetadata.class);

  public AbstractFileMetadata(FileStatus fileStatus, String sourcePath, Credentials credentials) {
    String fullPath = fileStatus.getPath().toString();
    String[] paths = fullPath.split("/");
    this.fileName = paths[paths.length - 1];
    this.timeStamp = fileStatus.getModificationTime();
    this.owner = fileStatus.getOwner();
    this.fileSize = fileStatus.getLen();
    this.credentials = credentials;
    this.permission = fileStatus.getPermission().toShort();

    // generate file folder
    this.fullPath = fullPath;

    // check if file is a folder
    this.isFolder = fileStatus.isDirectory();

    // TODO: investigate how to cleanly set basePath
    if (sourcePath.equals("")) {
      basePath = this.fileName;
    } else {
      /*
       * this block of code calculates how many folders (separated by "/")
       * should be dropped from the prefix
       */
      int numSourcePaths = sourcePath.split("/").length;
      int numStrip;
      if (sourcePath.endsWith("/")) {
        numStrip = numSourcePaths;
        sourcePath = sourcePath.substring(0, sourcePath.length() - 1);
      } else {
        numStrip = numSourcePaths - 1;
      }

      /*
       * this block of code drops the URI and reconstructs basePath by
       * concatenating the folders with "/"
       */
      String pathWithoutURI = fullPath.substring(fullPath.indexOf(sourcePath));
      String[] pathsWithoutURI = pathWithoutURI.split("/");
      basePath = "";
      for (int i = numStrip; i < pathsWithoutURI.length; i++) {
        basePath = basePath.concat(pathsWithoutURI[i] + "/");
      }
      if (basePath.length() > 0) {
        basePath = basePath.substring(0, basePath.length() - 1);
      }
    }
  }

  public AbstractFileMetadata(String fileName, String fullPath, long timeStamp, String owner,
                              Long fileSize, Boolean isFolder, String basePath,
                              short permission, Credentials credentials) {
    this.fileName = fileName;
    this.fullPath = fullPath;
    this.timeStamp = timeStamp;
    this.owner = owner;
    this.fileSize = fileSize;
    this.credentials = credentials;
    this.isFolder = isFolder;
    this.basePath = basePath;
    this.permission = permission;
  }

  public String getFullPath() {
    return fullPath;
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileSize() {
    return fileSize;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public String getOwner() {
    return owner;
  }

  public abstract Credentials getCredentials();

  public Boolean getIsFolder() {
    return this.isFolder;
  }

  public String getBasePath() {
    return basePath;
  }

  public short getPermission() {
    return permission;
  }

  /**
   * extend from this class to create a credential class for each filesystem
   */
  public abstract static class Credentials {
    public String databaseType;
    public static final String DATA_BASE_TYPE = "databaseType";
  }
}
