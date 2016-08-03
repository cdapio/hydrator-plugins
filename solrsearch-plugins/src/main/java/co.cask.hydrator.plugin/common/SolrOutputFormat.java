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
package co.cask.hydrator.plugin.common;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * SolrOutputFormat - Output format class for Solr.
 */
public class SolrOutputFormat extends OutputFormat {
  @Override
  public SolrRecordWriter getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new SolrRecordWriter(context);
  }

  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    // none
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {

    //return an empty outputcommitter
    return new OutputCommitter() {
      @Override
      public void setupTask(TaskAttemptContext arg0) throws IOException {
      }

      @Override
      public void setupJob(JobContext arg0) throws IOException {
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext arg0) throws IOException {
      }

      @Override
      public void abortTask(TaskAttemptContext arg0) throws IOException {
      }
    };
  }
}
