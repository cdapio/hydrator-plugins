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

package co.cask.hydrator.plugin.batch.source;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Abhinav on 9/1/16.
 */
public class UncRecordReader extends RecordReader<LongWritable, Text> {

  private Text currentValue;
  private LongWritable currentKey;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    String user = "vagrant";
    String sharedFolder = "abhi";
    InputStream in = null;
    try {
      String path = "smb://10.150.3.167/" + sharedFolder + "/test.txt";
      NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", user, user);
      SmbFile smbFile = new SmbFile(path, auth);

      in = smbFile.getInputStream();
      BufferedInputStream bis = new BufferedInputStream(in);
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      int result = bis.read();
      while (result != -1) {
        buf.write((byte) result);
        result = bis.read();
      }
      currentValue = new Text(buf.toString());
    } catch (Exception e) {
    } finally {
      if (in != null) {
        in.close();
      }
      return false;
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return currentValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }
}
