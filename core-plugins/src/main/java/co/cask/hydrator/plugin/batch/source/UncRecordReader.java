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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by Abhinav on 9/1/16.
 */
public class UncRecordReader extends RecordReader<LongWritable, Text> {

  private Text currentValue;
  private long counter;
  private LongWritable currentKey;
  private ByteArrayOutputStream buf;
  private BufferedReader bis;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    String user = "vagrant";
    String sharedFolder = "abhi";
    InputStream in = null;
    String path = "smb://10.150.3.167/" + sharedFolder + "/test.txt";
    NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", user, user);
    SmbFile smbFile = new SmbFile(path, auth);

    in = smbFile.getInputStream();
    bis = new BufferedReader(new InputStreamReader(in));
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    String line = null;
    try {
      line = bis.readLine();
      if (line != null) {
        currentValue = new Text(line);
        counter = counter + 1;
        currentKey = new LongWritable(counter);
        return true;
      } else {
        bis.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return currentKey;
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
