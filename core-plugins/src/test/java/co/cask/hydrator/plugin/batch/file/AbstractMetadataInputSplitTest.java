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
import co.cask.hydrator.plugin.batch.file.s3.S3MetadataInputSplit;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AbstractMetadataInputSplitTest {
  @Test
  public void testSerializeAndDeserialize() throws Exception {
    // required for abstract metadata
    final long lengthA = 101;
    final long lengthB = 202;
    final boolean isdir = false;
    final int blockReplication = 0;
    final long blocksize = 0;
    final long modificationTime = 12345678;
    final long accessTime = 87654321;
    final FsPermission permission = new FsPermission((short) 166);
    final String owner  = "someOwner";
    final String group = "someGroup";
    final Path path = new Path("s3a://abc.def.ghi/foo/bar/baz/123.txt");
    final String sourcePath = "/foo/bar";

    // required for s3metadata
    final String accessKeyId = "akey";
    final String secretKeyId = "skey";
    final String region = "us-east-1";

    // initialize an inputSplit
    FileStatus fileStatusA = new FileStatus(lengthA, isdir, blockReplication, blocksize,
                                           modificationTime, accessTime, permission, owner, group, path);
    FileStatus fileStatusB = new FileStatus(lengthB, isdir, blockReplication, blocksize,
                                            modificationTime, accessTime, permission, owner, group, path);
    S3FileMetadata originalMetadataA = new S3FileMetadata(fileStatusA, sourcePath, accessKeyId, secretKeyId, region);
    S3FileMetadata originalMetadataB = new S3FileMetadata(fileStatusB, sourcePath, accessKeyId, secretKeyId, region);
    S3MetadataInputSplit metadataInputSplit = new S3MetadataInputSplit();
    metadataInputSplit.addFileMetadata(originalMetadataA);
    metadataInputSplit.addFileMetadata(originalMetadataB);

    // serialize input split
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    metadataInputSplit.write(outputStream);
    outputStream.flush();
    outputStream.close();
    byte[] serialized = byteArrayOutputStream.toByteArray();

    // deserialize input split
    S3MetadataInputSplit recoveredInputSplit = new S3MetadataInputSplit();
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialized);
    DataInputStream inputStream = new DataInputStream(byteArrayInputStream);
    recoveredInputSplit.readFields(inputStream);
    inputStream.close();

    // compare if split size is right
    Assert.assertEquals(metadataInputSplit.getLength(), recoveredInputSplit.getLength());
    Assert.assertEquals(metadataInputSplit.getTotalBytes(), recoveredInputSplit.getTotalBytes());

    // compare if recovered fileMetadata is right
    Assert.assertEquals(recoveredInputSplit.getFileMetaDataList().get(0).toRecord(),
                        metadataInputSplit.getFileMetaDataList().get(0).toRecord());
    Assert.assertEquals(recoveredInputSplit.getFileMetaDataList().get(1).toRecord(),
                        metadataInputSplit.getFileMetaDataList().get(1).toRecord());
  }

  @Test
  public void testCompare() throws IOException {
    S3MetadataInputSplit metadataInputSplita = new S3MetadataInputSplit();
    S3MetadataInputSplit metadataInputSplitb = new S3MetadataInputSplit();
    S3MetadataInputSplit metadataInputSplitc = new S3MetadataInputSplit();

    final FileStatus statusA = new FileStatus(1, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileA"));
    final FileStatus statusB = new FileStatus(2, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileB"));
    final FileStatus statusC = new FileStatus(3, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileC"));
    final String basePath = "/abc";

    // required for s3metadata
    final String accessKeyId = "akey";
    final String secretKeyId = "skey";
    final String region = "us-east-1";

    // generate 3 files with different file sizes
    S3FileMetadata file1 = new S3FileMetadata(statusA, basePath, accessKeyId, secretKeyId, region);

    S3FileMetadata file2 = new S3FileMetadata(statusB, basePath, accessKeyId, secretKeyId, region);

    S3FileMetadata file3 = new S3FileMetadata(statusC, basePath, accessKeyId, secretKeyId, region);

    // a has 3 bytes
    metadataInputSplita.addFileMetadata(file1);
    metadataInputSplita.addFileMetadata(file2);

    // b has 3 bytes too
    metadataInputSplitb.addFileMetadata(file3);

    // c only has 1 byte
    metadataInputSplitc.addFileMetadata(file1);

    Assert.assertEquals(metadataInputSplita.compareTo(metadataInputSplitb), 0);
    Assert.assertEquals(metadataInputSplita.compareTo(metadataInputSplitc), 1);
    Assert.assertEquals(metadataInputSplitc.compareTo(metadataInputSplita), -1);
  }
}
