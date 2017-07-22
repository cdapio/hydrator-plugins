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

import co.cask.hydrator.plugin.batch.file.s3.S3FileMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * The record writer that takes file metadata and streams data from source database
 * to destination database
 */
public class FileCopyRecordWriter extends RecordWriter<NullWritable, AbstractFileMetadata> {
  private FileSystem destFileSystem;
  private String basePath;
  private Boolean enableOverwrite;
  private int bufferSize;
  // buffer size defaults to 1 MB
  private static final int defaultBufferSize = 2 << 20;
  private static final Logger LOG = LoggerFactory.getLogger(FileCopyRecordWriter.class);

  public FileCopyRecordWriter(Configuration conf)
    throws Exception {
    this.destFileSystem = FileSystem.get(conf);
    this.basePath = conf.get(FileCopyOutputFormat.BASE_PATH);
    this.enableOverwrite = conf.getBoolean(FileCopyOutputFormat.ENABLE_OVERWRITE, false);
    this.bufferSize = conf.getInt(FileCopyOutputFormat.BUFFER_SIZE, defaultBufferSize);
    this.sourceFilesystemMap = new HashMap<>();
  }

  // source filesystems
  private Map<AbstractFileMetadata.Credentials, FileSystem> sourceFilesystemMap;

  @Override
  public void write(NullWritable key, AbstractFileMetadata fileMetadata) throws IOException, InterruptedException {
    // get source database connection
    AbstractFileMetadata.Credentials sourceCredentials = fileMetadata.getCredentials();
    if (!sourceFilesystemMap.containsKey(sourceCredentials)) {
      sourceFilesystemMap.put(sourceCredentials, getSourceFilesystemFromCredentials(sourceCredentials));
    }
    FileSystem sourceFilesystem = sourceFilesystemMap.get(sourceCredentials);

    // construct file paths for source and destination
    Path srcPath = new Path(fileMetadata.fullPath);
    Path destPath = new Path(basePath + "/" + fileMetadata.basePath);
    FsPermission permission = new FsPermission(fileMetadata.getPermission());

    if (fileMetadata.isFolder) {
      // create an empty folder and return
      if (!destFileSystem.exists(destPath) && sourceFilesystem.isDirectory(srcPath)) {
        destFileSystem.mkdirs(destPath, permission);
      }
      return;
    } else if (!sourceFilesystem.exists(srcPath)) {
      // file doesn't exist in source, return immediately
      return;
    }

    // immediately return if we dont want to overwrite and file exists in destination
    if (!enableOverwrite && destFileSystem.exists(destPath)) {
      return;
    }

    // data streaming
    FSDataInputStream inputStream = sourceFilesystem.open(srcPath);
    FSDataOutputStream outputStream = FileSystem.create(destFileSystem, destPath, permission);
    byte[] buf = new byte[bufferSize];
    int len;
    while ((len = inputStream.read(buf)) >= 0) {
      outputStream.write(buf, 0, len);
    }
    inputStream.close();
    outputStream.close();
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    /*
     * TODO: investigate when to close the destination filesystem and the source filesystems
     * We can't close the filesystems here because the filesystem instantiation is shared across
     * multiple splits. If one split closes the connection, other splits will fail to transfer.
     */
  }

  private FileSystem getSourceFilesystemFromCredentials(AbstractFileMetadata.Credentials credentials)
    throws IOException {
    Configuration conf = new Configuration(false);
    conf.clear();
    switch (credentials.databaseType) {
      case S3FileMetadata.DATA_BASE_NAME :
        S3FileMetadata.S3Credentials s3Credentials = (S3FileMetadata.S3Credentials) credentials;
        conf.set("fs.s3a.access.key", s3Credentials.accessKeyId);
        conf.set("fs.s3a.secret.key", s3Credentials.secretKeyId);
        conf.set("fs.s3a.impl", S3AFileSystem.class.getName());
        URI uri = URI.create("s3a://" + s3Credentials.bucketName);
        return NativeS3FileSystem.get(uri, conf);
    }
    return null;
  }
}
