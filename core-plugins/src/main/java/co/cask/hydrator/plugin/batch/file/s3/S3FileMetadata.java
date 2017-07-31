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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.file.AbstractFileMetadata;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Filemetadata specific for S3. Defines credentials that are required for
 * connecting to S3.
 */
public class S3FileMetadata extends AbstractFileMetadata {

  public static final String ACCESS_KEY_ID = "accessKeyID";
  public static final String SECRET_KEY_ID = "secretKeyID";
  public static final String REGION = "region";
  public static final Schema CREDENTIAL_SCHEMA = Schema.recordOf(
    "metadata",
    Schema.Field.of(ACCESS_KEY_ID, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(SECRET_KEY_ID, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(REGION, Schema.of(Schema.Type.STRING))
  );

  private final String accessKeyId;
  private final String secretKeyId;
  private final String region;

  private static final Logger LOG = LoggerFactory.getLogger(S3FileMetadata.class);

  public S3FileMetadata(FileStatus fileStatus, String sourcePath,
                        String accessKeyId, String secretKeyId, String region) throws IOException {
    super(fileStatus, sourcePath);
    this.accessKeyId = accessKeyId;
    this.secretKeyId = secretKeyId;
    this.region = region;
  }

  public S3FileMetadata(StructuredRecord record) {
    super(record);
    this.accessKeyId = record.get(ACCESS_KEY_ID);
    this.secretKeyId = record.get(SECRET_KEY_ID);
    this.region = record.get(REGION);
  }

  public S3FileMetadata(DataInput input) throws IOException {
    super(input);
    this.accessKeyId = input.readUTF();
    this.secretKeyId = input.readUTF();
    this.region = input.readUTF();
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getSecretKeyId() {
    return secretKeyId;
  }

  public String getRegion() {
    return region;
  }

  @Override
  protected Schema getCredentialSchema() {
    return CREDENTIAL_SCHEMA;
  }

  @Override
  protected void addCredentialsToBuilder(StructuredRecord.Builder builder) {
    builder
      .set(ACCESS_KEY_ID, accessKeyId)
      .set(SECRET_KEY_ID, secretKeyId)
      .set(REGION, region);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);
    dataOutput.writeUTF(accessKeyId);
    dataOutput.writeUTF(secretKeyId);
    dataOutput.writeUTF(region);
  }
}
