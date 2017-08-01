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
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * MetadataInputFormat for S3 Filesystem. Implements credentials setters
 * specific for S3
 */
public class S3MetadataInputFormat extends AbstractMetadataInputFormat {

  // configs for s3a
  public static final String S3A_ACCESS_KEY_ID = "fs.s3a.access.key";
  public static final String S3A_SECRET_KEY_ID = "fs.s3a.secret.key";
  public static final String S3A_FS_CLASS = "fs.s3a.impl";

  // configs for s3n
  public static final String S3N_ACCESS_KEY_ID = "fs.s3n.awsAccessKeyId";
  public static final String S3N_SECRET_KEY_ID = "fs.s3n.awsSecretAccessKey";
  public static final String S3N_FS_CLASS = "fs.s3n.impl";

  public static final String REGION = "amazons3.region";
  public static final Logger LOG = LoggerFactory.getLogger(S3MetadataInputFormat.class);


  public static void setS3aAccessKeyId(Configuration conf, String value) {
    conf.set(S3A_ACCESS_KEY_ID, value);
  }

  public static void setS3aSecretKeyId(Configuration conf, String value) {
    conf.set(S3A_SECRET_KEY_ID, value);
  }

  public static void setS3aFsClass(Configuration conf) {
    conf.set(S3A_FS_CLASS, S3AFileSystem.class.getName());
  }

  public static void setS3nAccessKeyId(Configuration conf, String value) {
    conf.set(S3N_ACCESS_KEY_ID, value);
  }

  public static void setS3nSecretKeyId(Configuration conf, String value) {
    conf.set(S3N_SECRET_KEY_ID, value);
  }

  public static void setS3nFsClass(Configuration conf) {
    conf.set(S3N_FS_CLASS, NativeS3FileSystem.class.getName());
  }

  public static void setRegion(Configuration conf, String value) {
    conf.set(REGION, value);
  }

  @Override
  protected AbstractMetadataInputSplit getInputSplit() {
    return new S3MetadataInputSplit();
  }

  @Override
  protected AbstractFileMetadata getFileMetadata(FileStatus fileStatus, String sourcePath, Configuration conf)
    throws IOException {
    switch (fileStatus.getPath().toUri().getScheme()) {
      case "s3a":
        return new S3FileMetadata(fileStatus, sourcePath,
                                  conf.get(S3A_ACCESS_KEY_ID), conf.get(S3A_SECRET_KEY_ID), conf.get(REGION));
      case "s3n":
        return new S3FileMetadata(fileStatus, sourcePath,
                                  conf.get(S3N_ACCESS_KEY_ID), conf.get(S3N_SECRET_KEY_ID), conf.get(REGION));
      default:
        throw new IOException("Scheme must be either s3a or s3n.");
    }
  }
}
