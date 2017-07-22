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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Returns key that contains file path.
 * Returns value that contains file attributes.
 */
public class MetadataRecordReader extends RecordReader<NullWritable, AbstractFileMetadata> {

  protected int currentIndex;
  protected AbstractMetadataInputSplit split;

  public MetadataRecordReader() {
    super();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if ((currentIndex + 1) < split.getLength()) {
      currentIndex++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return null;
  }


  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (1 - ((float) currentIndex / split.getLength()));
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {

    // sorry I should think of a better way instead of casting
    this.split = (AbstractMetadataInputSplit) inputSplit;

    this.currentIndex = -1;
  }

  @Override
  public AbstractFileMetadata getCurrentValue() throws IOException, InterruptedException {
    return split.getFileMetaDataList().get(currentIndex);
  }

  @Override
  public void close() throws IOException {

  }
}
