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

package co.cask.hydrator.plugin.batch.sink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 * OutputFormat for writing tar.gz file to configured hdfs path per mapper. It uses taskAttemptId to differentiate
 * between output of each mapper
 */
public class BatchTarSinkOutputFormat extends FileOutputFormat<Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchTarSinkOutputFormat.class);

  @Override
  public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

    return new RecordWriter<Text, Text>() {

      @Override
      public void write(Text key, Text value) throws IOException, InterruptedException {
        // create temp files on container
        String fileName = key.toString();
        String data = value.toString();
        File baseDir = new File(System.getProperty("user.dir") + "/tar");
        baseDir.mkdir();
        File outfile = new File(baseDir + "/" + fileName);
        outfile.createNewFile();
        LOG.info("Writing to {}", outfile.getAbsolutePath());
        FileOutputStream fop = new FileOutputStream(outfile);
        fop.write(data.getBytes());
        fop.flush();
        fop.close();
        LOG.info("Closed file {}", outfile.getAbsolutePath());
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // There is only one RecordWriter per mapper writing to hdfs. So this method will tar the files written in
        // write() method.
        try {
          tarZip(context);
        } catch (Exception e) {
          LOG.info("Exception in zip", e);
        }
      }

      private void tarZip(TaskAttemptContext context) throws Exception {
        Configuration conf = context.getConfiguration();
        // temp output file to store tar.gz file on container's local directory
        File output = new File(System.getProperty("user.dir") + "/output");
        output.mkdir();
        File baseDir = new File(System.getProperty("user.dir") + "/tar");
        // Tar zip the file on containers local directory and copy it to hdfs
        ProcessBuilder processBuilder = new ProcessBuilder("tar", "-czf", "archive.tar.gz", "-C",
                                                           baseDir.getPath(), ".");
        processBuilder.directory(output);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        int exitValue = process.waitFor();
        LOG.info("Exit value of Process is {}", exitValue);
        if (exitValue != 0) {
          LOG.warn("Returning without copying to hdfs");
          return;
        }
        LOG.info("Copying tar file to hdfs");
        // copy tar.gz file to hdfs
        FileSystem distributedFileSystem = FileSystem.get(conf);
        FileUtil.copy(output, distributedFileSystem, new
          Path(conf.get(BatchTarSink.TAR_TARGET_PATH) + "/" + context.getTaskAttemptID()), false, conf);
      }
    };
  }
}
