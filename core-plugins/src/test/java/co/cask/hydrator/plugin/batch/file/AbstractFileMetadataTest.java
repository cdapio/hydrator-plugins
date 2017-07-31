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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class AbstractFileMetadataTest {
  @Test
  public void testConvertFileStatusToFileMetaData() throws IOException {
    FileStatus fileStatus = new FileStatus();
    fileStatus.setPath(new Path("s3a://abc.def.bucket/source/path/directory/123.txt"));

    // Copy a file that is part of a whole directory copy
    String sourcePath = "/source/path/directory";
    S3FileMetadata metadata = new S3FileMetadata(fileStatus, sourcePath, null, null, null);
    Assert.assertEquals(metadata.getFileName(), "123.txt");
    Assert.assertEquals(metadata.getFullPath(), "s3a://abc.def.bucket/source/path/directory/123.txt");
    Assert.assertEquals(metadata.getRelativePath(), "directory/123.txt");
    Assert.assertEquals(metadata.getHostURI(), "s3a://abc.def.bucket");

    // Copy a file that is part of a whole directory copy without including the directory
    sourcePath = "/source/path/";
    metadata = new S3FileMetadata(fileStatus, sourcePath, null, null, null);
    Assert.assertEquals(metadata.getFileName(), "123.txt");
    Assert.assertEquals(metadata.getFullPath(), "s3a://abc.def.bucket/source/path/directory/123.txt");
    Assert.assertEquals(metadata.getRelativePath(), "directory/123.txt");
    Assert.assertEquals(metadata.getHostURI(), "s3a://abc.def.bucket");

    fileStatus.setPath(new Path("s3a://abc.def.bucket/"));
    sourcePath = "/";
    metadata = new S3FileMetadata(fileStatus, sourcePath, null, null, null);
    Assert.assertEquals(metadata.getRelativePath().isEmpty(), true);

    fileStatus.setPath(new Path("s3a://abc.def.bucket/abc.txt"));
    sourcePath = "/";
    metadata = new S3FileMetadata(fileStatus, sourcePath, null, null, null);
    Assert.assertEquals(metadata.getRelativePath(), "abc.txt");
  }

  @Test
  public void testCompare() throws IOException {
    final FileStatus statusA = new FileStatus(1, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileA"));
    final FileStatus statusB = new FileStatus(3, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileB"));
    final FileStatus statusC = new FileStatus(3, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileC"));
    final String basePath = "/abc";

    // generate 3 files with different file sizes
    S3FileMetadata file1 = new S3FileMetadata(statusA, basePath, null, null, null);

    S3FileMetadata file2 = new S3FileMetadata(statusB, basePath, null, null, null);

    S3FileMetadata file3 = new S3FileMetadata(statusC, basePath, null, null, null);

    Assert.assertEquals(file1.compareTo(file2), -1);
    Assert.assertEquals(file3.compareTo(file2), 0);
    Assert.assertEquals(file3.compareTo(file1), 1);
  }
}
