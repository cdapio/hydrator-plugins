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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by Abhinav on 8/31/16.
 */
public class UncInputFormat extends FileInputFormat {

  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String UNCPATH = "uncpath";

  public static void setConfigurations(Job job, String username, String password, String uncpath) {
    Configuration configuration = job.getConfiguration();
    configuration.set(USERNAME, username);
    configuration.set(PASSWORD, password);
    configuration.set(UNCPATH, uncpath);
  }


  @Override
  public RecordReader<LongWritable, Text> createRecordReader
    (InputSplit split, TaskAttemptContext context) throws IOException {
    return new UncRecordReader();
  }

  protected boolean isSplitable(JobContext context, Path file) {
    //for now we are not splitting the files
    return false;
  }

  //TODO override makeSplit and getSplits method to get rid of the local path
    /* * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
/*  protected FileSplit makeSplit(Path file, long start, long length,
                                String[] hosts) {
    return new FileSplit(file, start, length, hosts);
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      splits.add(makeSplit(path, 0, length, new String[0]));
    }
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
    return splits;
  }*/

  /**
   *
   */
  public static class UncRecordReader extends RecordReader<LongWritable, Text> {

    private Text currentValue;
    private long counter;
    private LongWritable currentKey;
    private ByteArrayOutputStream buf;
    private BufferedReader bis;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      Configuration job = context.getConfiguration();

      InputStream in = null;
      String user = job.get(USERNAME);
      String password = job.get(PASSWORD);
      String path = job.get(UNCPATH);

      NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("", user, password);
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

}
