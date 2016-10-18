/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * whole file record reader
 */
public class WholeFileRecordReader extends RecordReader<String, BytesWritable> {
  private FileSplit fileSplit;
  private Configuration conf;
  private boolean processed = false;

  private NullWritable key = NullWritable.get();
  private BytesWritable value = new BytesWritable();

  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    this.fileSplit = (FileSplit) inputSplit;
    this.conf = taskAttemptContext.getConfiguration();
  }

  public boolean nextKeyValue() throws IOException {
    if (!processed) {
      byte[] contents = new byte[(int) fileSplit.getLength()];

      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(conf);

      FSDataInputStream in = null;
      try {
        in = fs.open(file);
        IOUtils.readFully(in, contents, 0, contents.length);
        value.set(contents, 0, contents.length);
      } finally {
        IOUtils.closeStream(in);
      }
      processed = true;
      return true;
    }
    return false;
  }

  @Override
  public String getCurrentKey() throws IOException, InterruptedException {
    return fileSplit.getPath().getName();
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException  {
    return processed ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}
