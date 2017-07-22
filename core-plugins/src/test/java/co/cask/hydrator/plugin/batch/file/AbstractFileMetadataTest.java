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

public class AbstractFileMetadataTest {

  @Test
  public void testConvertFileStatusToFileMetaData() {
    FileStatus fileStatus = new FileStatus();
    fileStatus.setPath(new Path("s3a://abc.def.bucket/source/path/directory/123.txt"));

    // Copy a file that is part of a whole directory copy
    String sourcePath = "/source/path/directory";
    S3FileMetadata.S3Credentials credentials = new S3FileMetadata.S3Credentials(null, null, null, null);
    S3FileMetadata metadata = new S3FileMetadata(fileStatus, sourcePath, credentials);
    Assert.assertEquals(metadata.getFileName(), "123.txt");
    Assert.assertEquals(metadata.getFullPath(), "s3a://abc.def.bucket/source/path/directory/123.txt");
    Assert.assertEquals(metadata.getBasePath(), "directory/123.txt");

    // Copy a file that is part of a whole directory copy without including the directory
    sourcePath = "/source/path/";
    credentials = new S3FileMetadata.S3Credentials(null, null, null, null);
    metadata = new S3FileMetadata(fileStatus, sourcePath, credentials);
    Assert.assertEquals(metadata.getFileName(), "123.txt");
    Assert.assertEquals(metadata.getFullPath(), "s3a://abc.def.bucket/source/path/directory/123.txt");
    Assert.assertEquals(metadata.getBasePath(), "directory/123.txt");

    // Copy a single file
    sourcePath = "";
    credentials = new S3FileMetadata.S3Credentials(null, null, null, null);
    metadata = new S3FileMetadata(fileStatus, sourcePath, credentials);
    Assert.assertEquals(metadata.getFileName(), "123.txt");
    Assert.assertEquals(metadata.getFullPath(), "s3a://abc.def.bucket/source/path/directory/123.txt");
    Assert.assertEquals(metadata.getBasePath(), "123.txt");
  }

}
