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
  private final FileSystem destFileSystem;
  private final String basePath;
  private final boolean enableOverwrite;
  private final boolean preserveOwner;
  private final boolean fsCache;
  private final int bufferSize;

  // buffer size defaults to 1 MB
  private static final int defaultBufferSize = 2 << 20;
  private static final Logger LOG = LoggerFactory.getLogger(FileCopyRecordWriter.class);

  // source filesystems
  private Map<String, FileSystem> sourceFilesystemMap;

  public FileCopyRecordWriter(Configuration conf) throws Exception {
    // connect to destination filesystem using uri if it is provided
    String uriString;
    if ((uriString = conf.get(FileCopyOutputFormat.FS_HOST_URI, null)) != null) {
      destFileSystem = FileSystem.get(URI.create(uriString), conf);
    } else {
      destFileSystem = FileSystem.get(conf);
    }
    basePath = conf.get(FileCopyOutputFormat.BASE_PATH);
    enableOverwrite = conf.getBoolean(FileCopyOutputFormat.ENABLE_OVERWRITE, false);
    preserveOwner = conf.getBoolean(FileCopyOutputFormat.PRESERVE_OWNER, false);
    fsCache = conf.getBoolean(FileCopyOutputFormat.FS_CACHE, true);
    bufferSize = conf.getInt(FileCopyOutputFormat.BUFFER_SIZE, defaultBufferSize);
    sourceFilesystemMap = new HashMap<>();
  }

  @Override
  public void write(NullWritable key, AbstractFileMetadata fileMetadata) throws IOException, InterruptedException {
    // construct file paths for source and destination
    Path srcPath = new Path(fileMetadata.getFullPath());
    Path destPath;
    if (fileMetadata.getBasePath().isEmpty()) {
      // nothing to create
      return;
    } else {
      destPath = new Path(basePath, fileMetadata.getBasePath());
    }
    FsPermission permission = new FsPermission(fileMetadata.getPermission());

    // immediately return if we don't want to overwrite and file exists in destination
    if (!enableOverwrite && destFileSystem.exists(destPath)) {
      return;
    }

    // get source database connection
    String uriString = fileMetadata.getHostURI();
    if (!sourceFilesystemMap.containsKey(uriString)) {
      sourceFilesystemMap.put(uriString, getSourceFilesystemConnection(fileMetadata, fsCache));
    }
    FileSystem sourceFilesystem = sourceFilesystemMap.get(uriString);

    if (fileMetadata.isFolder()) {
      // create an empty folder and return
      if (!destFileSystem.exists(destPath) && sourceFilesystem.isDirectory(srcPath)) {
        destFileSystem.mkdirs(destPath, permission);
        if (preserveOwner) {
          destFileSystem.setOwner(destPath, fileMetadata.getOwner(), fileMetadata.getGroup());
        }
      }
      return;
    } else if (!sourceFilesystem.exists(srcPath)) {
      // file doesn't exist in source, return immediately
      LOG.warn("{} doesn't exist in source filesystem.", fileMetadata.getFullPath());
      return;
    }

    // data streaming
    FSDataInputStream inputStream = sourceFilesystem.open(srcPath);
    FSDataOutputStream outputStream = FileSystem.create(destFileSystem, destPath, permission);
    try {
      byte[] buf = new byte[bufferSize];
      int len;
      while ((len = inputStream.read(buf)) >= 0) {
        outputStream.write(buf, 0, len);
      }
    } finally {
      if (preserveOwner) {
        destFileSystem.setOwner(destPath, fileMetadata.getOwner(), fileMetadata.getGroup());
      }
      inputStream.close();
      outputStream.close();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    /*
     * TODO: investigate when to close the destination filesystem and the source filesystems
     * if we use cached connections
     * We can't close the filesystems here because the filesystem cache is shared across
     * multiple splits. If one split closes the connection, other splits will fail to transfer.
     */
    if (!fsCache) {
      destFileSystem.close();
      for (Map.Entry<String, FileSystem> fs : sourceFilesystemMap.entrySet()) {
        fs.getValue().close();
      }
    }
  }

  private FileSystem getSourceFilesystemConnection(AbstractFileMetadata metadata, boolean cache)
    throws IOException {
    Configuration conf = new Configuration(false);
    conf.clear();

    // turn filesystem caching off
    URI uri = URI.create(metadata.getHostURI());

    // whether or not to disable caching for source filesystems
    String disableCacheName = String.format("fs.%s.impl.disable.cache", uri.getScheme());
    conf.set(disableCacheName, String.valueOf(!cache));

    switch (metadata.getFilesystem()) {
      case S3FileMetadata.FILESYSTEM_NAME :
        S3FileMetadata s3FileMetadata = (S3FileMetadata) metadata;
        conf.set("fs.s3a.access.key", s3FileMetadata.getAccessKeyId());
        conf.set("fs.s3a.secret.key", s3FileMetadata.getSecretKeyId());
        conf.set("fs.s3a.impl", S3AFileSystem.class.getName());
        return NativeS3FileSystem.get(uri, conf);
    }
    return null;
  }
}
