/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.sink.output;

import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * An {@link OutputFormat} which delegates to {@link TextOutputFormat} but in the
 * {@link OutputCommitter#commitJob(JobContext)} after delgating it also performs a bulk load to Vertica to through
 * copy command to write to a Vertica table.
 * @param <K> key class
 * @param <V> value class
 */
public class BulkOutputFormat<K, V> extends TextOutputFormat<K, V> {
  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    final OutputCommitter delegateCommitter = super.getOutputCommitter(context);
    return new OutputCommitter() {
      @Override
      public void commitJob(JobContext jobContext) throws IOException {
        delegateCommitter.commitJob(jobContext);

        // TODO: Write to vertica
      }

      @Override
      public void setupJob(JobContext jobContext) throws IOException {
        delegateCommitter.setupJob(jobContext);
      }

      @Override
      public void setupTask(TaskAttemptContext taskContext) throws IOException {
        delegateCommitter.setupTask(taskContext);
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return delegateCommitter.needsTaskCommit(taskContext);
      }

      @Override
      public void commitTask(TaskAttemptContext taskContext) throws IOException {
        delegateCommitter.commitTask(taskContext);
      }

      @Override
      public void abortTask(TaskAttemptContext taskContext) throws IOException {
        delegateCommitter.abortTask(taskContext);
      }
    };
  }
}
