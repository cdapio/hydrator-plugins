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
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class AbstractMetadataInputSplitTest {
  @Test
  public void testSerializeAndDeserialize() throws Exception {
    S3MetadataInputSplit metadataInputSplit = new S3MetadataInputSplit();

    S3FileMetadata.S3Credentials credentials = new S3FileMetadata.S3Credentials("akey", "skey", "region", "bname");
    S3FileMetadata originalMetadata = new S3FileMetadata("fileName",
                                                         "fullPath",
                                                         123456,
                                                         "owner",
                                                         (long) 678910,
                                                         false,
                                                         "basePath",
                                                         (short) 166,
                                                         credentials);
    metadataInputSplit.addFileMetadata(originalMetadata);

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

    // compare if fields are right
    S3FileMetadata recoveredMetadata = (S3FileMetadata) recoveredInputSplit.getFileMetaDataList().get(0);
    Assert.assertEquals(recoveredMetadata.getFileName(), "fileName");
    Assert.assertEquals(recoveredMetadata.getFullPath(), "fullPath");
    Assert.assertEquals(recoveredMetadata.getTimeStamp(), 123456);
    Assert.assertEquals(recoveredMetadata.getOwner(), "owner");
    Assert.assertEquals(recoveredMetadata.getFileSize(), (long) 678910);
    Assert.assertEquals(recoveredMetadata.getIsFolder(), false);
    Assert.assertEquals(recoveredMetadata.getBasePath(), "basePath");
    Assert.assertEquals(recoveredMetadata.getPermission(), (short) 166);
    Assert.assertEquals(recoveredMetadata.getCredentials().accessKeyId, "akey");
    Assert.assertEquals(recoveredMetadata.getCredentials().secretKeyId, "skey");
    Assert.assertEquals(recoveredMetadata.getCredentials().region, "region");
    Assert.assertEquals(recoveredMetadata.getCredentials().bucketName, "bname");
  }

  @Test
  public void testCompare() {
    S3MetadataInputSplit metadataInputSplita = new S3MetadataInputSplit();
    S3MetadataInputSplit metadataInputSplitb = new S3MetadataInputSplit();
    S3MetadataInputSplit metadataInputSplitc = new S3MetadataInputSplit();

    // generate 3 files with different file sizes
    S3FileMetadata file1 = new S3FileMetadata(null, null, 0, null, (long) 1, false, null, (short) 0, null);

    S3FileMetadata file2 = new S3FileMetadata(null, null, 0, null, (long) 2, false, null, (short) 0, null);

    S3FileMetadata file3 = new S3FileMetadata(null, null, 0, null, (long) 3, false, null, (short) 0, null);

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
