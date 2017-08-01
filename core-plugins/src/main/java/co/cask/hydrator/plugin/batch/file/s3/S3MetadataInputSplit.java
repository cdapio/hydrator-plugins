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

import java.io.DataInput;
import java.io.IOException;
import java.util.List;


/**
 * InputSplit that implements methods for serializing S3 AbstractCredentials
 */
public class S3MetadataInputSplit extends AbstractMetadataInputSplit {
  public S3MetadataInputSplit(List<AbstractFileMetadata> s3FileMetadataList) {
    super(s3FileMetadataList);
  }

  public S3MetadataInputSplit() {
    super();
  }

  @Override
  protected AbstractFileMetadata readFileMetaData(DataInput dataInput) throws IOException {
    return new S3FileMetadata(dataInput);
  }
}
