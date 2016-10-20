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

package co.cask.hydrator.plugin.sink;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/**
 * OutputFormat for writing tar.gz file to configured hdfs path per mapper. It uses taskAttemptId to differentiate
 * between output of each mapper
 */
public class BatchTarSinkOutputFormat extends OutputFormat<Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchTarSinkOutputFormat.class);

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    return new OutputCommitter() {
      @Override
      public void setupJob(final JobContext jobContext) throws IOException {
        // DO NOTHING, see needsTaskCommit() comment
      }

      @Override
      public boolean needsTaskCommit(final TaskAttemptContext taskContext) throws IOException {
        // Don't do commit of individual task work. Work is committed on job level. Ops are flushed on
        // RecordWriter.close.
        return false;
      }

      @Override
      public void setupTask(final TaskAttemptContext taskContext) throws IOException {
        // DO NOTHING, see needsTaskCommit() comment
      }

      @Override
      public void commitTask(final TaskAttemptContext taskContext) throws IOException {
        // DO NOTHING, see needsTaskCommit() comment
      }

      @Override
      public void abortTask(final TaskAttemptContext taskContext) throws IOException {
        // DO NOTHING, see needsTaskCommit() comment
      }
    };
  }

  @Override
  public RecordWriter<Text, Text> getRecordWriter(final TaskAttemptContext job)
    throws IOException, InterruptedException {

    return new RecordWriter<Text, Text>() {

      @Override
      public void write(Text key, Text value) throws IOException, InterruptedException {
        Configuration configuration = job.getConfiguration();
        // create temp files on container
        String fileName = key.toString();
        String data = value.toString();
        File baseDir = new File(System.getProperty("user.dir") + "/tar");
        if (!baseDir.exists()) {
          baseDir.mkdir();
        }
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
          // TODO think about what happens when there is an exception in zip. Retry?
          LOG.error("Exception in zip", e);
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
        Path targetPath = new Path(conf.get(BatchTarSink.TAR_TARGET_PATH) + "/" + context.getTaskAttemptID());
        LOG.info("targetPath {}", targetPath.toUri().toString());
        LOG.info("output {}", output.getAbsolutePath());
        boolean hasWritten = FileUtil.copy(output, distributedFileSystem, targetPath, false, conf);

        if (hasWritten) {
          LOG.info("Tar file copied to hdfs successfully from mapper {}", context.getTaskAttemptID());
        } else {
          LOG.error("Not able to write tar file to hdfs from mapper {}", context.getTaskAttemptID());
        }
      }

      private Set<String> getFileExtensions(Configuration conf) {
        Set<String> set = new HashSet<>();
        for (String field : Splitter.on(',').trimResults().split(conf.get(BatchTarSink.SUPPORTED_EXTN))) {
          set.add(field);
        }
        return ImmutableSet.copyOf(set);
      }
    };


  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    // noop
  }
}
