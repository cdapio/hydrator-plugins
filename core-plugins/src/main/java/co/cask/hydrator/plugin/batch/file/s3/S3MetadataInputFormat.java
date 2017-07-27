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

package co.cask.hydrator.plugin.batch.file.s3;

import co.cask.hydrator.plugin.batch.file.AbstractFileMetadata;
import co.cask.hydrator.plugin.batch.file.AbstractMetadataInputFormat;
import co.cask.hydrator.plugin.batch.file.AbstractMetadataInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * MetadataInputFormat for S3 Filesystem. Implements credentials setters
 * specific for S3
 */
public class S3MetadataInputFormat extends AbstractMetadataInputFormat {

  private static final String ACCESS_KEY_ID = "fs.s3a.access.key";
  private static final String SECRET_KEY_ID = "fs.s3a.secret.key";
  private static final String FS_CLASS = "fs.s3a.impl";
  private static final String REGION = "amazons3.region";
  private static final Logger LOG = LoggerFactory.getLogger(S3MetadataInputFormat.class);


  public static void setAccessKeyId(Configuration conf, String value) {
    conf.set(ACCESS_KEY_ID, value);
  }

  public static void setSecretKeyId(Configuration conf, String value) {
    conf.set(SECRET_KEY_ID, value);
  }

  public static void setRegion(Configuration conf, String value) {
    conf.set(REGION, value);
  }

  public static void setFsClass(Configuration conf) {
    conf.set(FS_CLASS, S3AFileSystem.class.getName());
  }

  @Override
  protected AbstractMetadataInputSplit getInputSplit() {
    return new S3MetadataInputSplit();
  }

  @Override
  protected AbstractFileMetadata getFileMetaData(FileStatus fileStatus, String sourcePath, Configuration conf)
    throws IOException {
    return new S3FileMetadata(fileStatus, sourcePath,
                              conf.get(ACCESS_KEY_ID), conf.get(SECRET_KEY_ID), conf.get(REGION));
  }
}
