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
import co.cask.hydrator.plugin.batch.file.AbstractMetadataInputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;


/**
 * InputSplit that implements methods for serializing S3 Credentials
 */
public class S3MetadataInputSplit extends AbstractMetadataInputSplit {

  private static final Logger LOG = LoggerFactory.getLogger(S3MetadataInputSplit.class);

  public S3MetadataInputSplit(List<AbstractFileMetadata> s3FileMetadataList) {
    super(s3FileMetadataList);
  }

  public S3MetadataInputSplit() {
    super();
  }

  @Override
  protected AbstractFileMetadata getFileMetaData(String fileName,
                                                 String fileFolder,
                                                 long timeStamp,
                                                 String owner,
                                                 long fileSize,
                                                 Boolean isFolder,
                                                 String baseFolder,
                                                 short permission,
                                                 AbstractFileMetadata.Credentials credentials) {
    return new S3FileMetadata(fileName,
                              fileFolder,
                              timeStamp,
                              owner,
                              fileSize,
                              isFolder,
                              baseFolder,
                              permission,
                              (S3FileMetadata.S3Credentials) credentials);
  }

  @Override
  protected void writeCredentials(DataOutput dataOutput, AbstractFileMetadata.Credentials credentials)
    throws Exception {
    S3FileMetadata.S3Credentials s3Credentials = (S3FileMetadata.S3Credentials) credentials;
    dataOutput.writeUTF(s3Credentials.bucketName);
    dataOutput.writeUTF(s3Credentials.accessKeyId);
    dataOutput.writeUTF(s3Credentials.secretKeyId);
    dataOutput.writeUTF(s3Credentials.region);
  }

  @Override
  protected AbstractFileMetadata.Credentials readCredentials(DataInput dataInput) throws Exception {
    String bucketName = dataInput.readUTF();
    String accessKeyId = dataInput.readUTF();
    String secretKeyId = dataInput.readUTF();
    String region = dataInput.readUTF();
    return new S3FileMetadata.S3Credentials(accessKeyId, secretKeyId, region, bucketName);
  }
}
