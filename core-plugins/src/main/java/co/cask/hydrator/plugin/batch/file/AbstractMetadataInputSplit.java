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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class that implements information for InputSplit.
 * Contains a list of fileMetadata that is assigned to the specific split.
 */
public abstract class AbstractMetadataInputSplit extends InputSplit implements Writable, Comparable {
  protected List<AbstractFileMetadata> fileMetaDataList;
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetadataInputSplit.class);

  public AbstractMetadataInputSplit(List<AbstractFileMetadata> fileMetaDataList) {
    this.fileMetaDataList = fileMetaDataList;
  }

  public AbstractMetadataInputSplit() {
    this.fileMetaDataList = new ArrayList<>();
  }

  public List<AbstractFileMetadata> getFileMetaDataList() {
    return this.fileMetaDataList;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    try {
      // write obj summaries
      dataOutput.writeLong(this.getLength());
      for (int i = 0; i < this.getLength(); i++) {
        // convert each filestatus (serializable) to byte array
        AbstractFileMetadata fileMetaData = fileMetaDataList.get(i);

        dataOutput.writeUTF(fileMetaData.getFileName());
        dataOutput.writeUTF(fileMetaData.getFullPath());
        dataOutput.writeLong(fileMetaData.getTimeStamp());
        dataOutput.writeUTF(fileMetaData.getOwner());
        dataOutput.writeLong(fileMetaData.getFileSize());
        dataOutput.writeBoolean(fileMetaData.getIsFolder());
        dataOutput.writeUTF(fileMetaData.getBasePath());
        dataOutput.writeShort(fileMetaData.getPermission());

        writeCredentials(dataOutput, fileMetaData.getCredentials());
      }

    } catch (Exception e) {
      LOG.error("serialization failed");
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    try {
      long numObjects = dataInput.readLong();
      fileMetaDataList = new ArrayList<>();
      for (long i = 0; i < numObjects; i++) {
        String fileName = dataInput.readUTF();
        String fileFolder = dataInput.readUTF();
        long timeStamp = dataInput.readLong();
        String owner = dataInput.readUTF();
        long fileSize = dataInput.readLong();
        Boolean isFolder = dataInput.readBoolean();
        String baseFolder = dataInput.readUTF();
        short permission = dataInput.readShort();

        AbstractFileMetadata.Credentials credentials = readCredentials(dataInput);

        fileMetaDataList.add(getFileMetaData(fileName,
                                             fileFolder,
                                             timeStamp,
                                             owner,
                                             fileSize,
                                             isFolder,
                                             baseFolder,
                                             permission,
                                             credentials));
      }
    } catch (Exception e) {
      LOG.error("deserialization failed");
    }
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return fileMetaDataList.size();
  }

  public long getTotalSize() {
    long size = 0;
    for (AbstractFileMetadata fileMetaData : fileMetaDataList) {
      size += fileMetaData.getFileSize();
    }

    return size;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  public void addFileMetadata(AbstractFileMetadata fileMetaData) {
    fileMetaDataList.add(fileMetaData);
  }

  @Override
  public int compareTo(Object o) {
    AbstractMetadataInputSplit other = (AbstractMetadataInputSplit) o;
    long myLength = getTotalSize();
    long otherLength = other.getTotalSize();
    if (myLength < otherLength) {
      return -1;
    } else if (myLength == otherLength) {
      return 0;
    } else {
      return 1;
    }
  }

  protected abstract void writeCredentials(DataOutput dataOutput,
                                           AbstractFileMetadata.Credentials credentials) throws Exception;

  protected abstract AbstractFileMetadata.Credentials readCredentials(DataInput dataInput)
    throws Exception;

  protected abstract AbstractFileMetadata getFileMetaData(String fileName,
                                                          String fileFolder,
                                                          long timeStamp,
                                                          String owner,
                                                          long fileSize,
                                                          Boolean isFolder,
                                                          String baseFolder,
                                                          short permission,
                                                          AbstractFileMetadata.Credentials credentials);
}


