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
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetadataInputFormat for S3 Filesystem. Implements credentials setters
 * specific for S3
 */
public class S3MetadataInputFormat extends AbstractMetadataInputFormat {

  private static final String ACCESS_KEY_ID = "fs.s3a.access.key";
  private static final String SECRET_KEY_ID = "fs.s3a.secret.key";
  private static final String FS_CLASS = "fs.s3a.impl";
  private static final String REGION = "amazons3.region";
  private static final String BUCKET_NAME = "amazons3.bucket.name";
  private static final Logger LOG = LoggerFactory.getLogger(S3MetadataInputFormat.class);


  public static void setAccessKeyId(Job job, String value) {
    job.getConfiguration().set(ACCESS_KEY_ID, value);
  }

  public static void setSecretKeyId(Job job, String value) {
    job.getConfiguration().set(SECRET_KEY_ID, value);
  }

  public static void setRegion(Job job, String value) {
    job.getConfiguration().set(REGION, value);
  }

  public static void setFsClass(Job job) {
    job.getConfiguration().set(FS_CLASS, S3AFileSystem.class.getName());
  }

  public static void setBucketName(Job job, String value) {
    job.getConfiguration().set(BUCKET_NAME, value);
  }

  @Override
  protected AbstractFileMetadata.Credentials getCredentialsFromConf(Configuration conf) {
    String bucketName = conf.get(BUCKET_NAME);
    String accessKeyId = conf.get(ACCESS_KEY_ID);
    String region = conf.get(REGION);
    String secretKeyId = conf.get(SECRET_KEY_ID);
    return new S3FileMetadata.S3Credentials(accessKeyId, secretKeyId, region, bucketName);
  }

  @Override
  protected AbstractMetadataInputSplit getInputSplit() {
    return new S3MetadataInputSplit();
  }

  @Override
  protected AbstractFileMetadata getFileMetaData(FileStatus fileStatus, String sourcePath,
                                                 AbstractFileMetadata.Credentials credentials) {
    return new S3FileMetadata(fileStatus, sourcePath, (S3FileMetadata.S3Credentials) credentials);
  }
}
