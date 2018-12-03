/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.output;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.function.Function;

/**
 * A output format that transforms a StructuredRecord into some other object, then delegates writing to another
 * output format.
 *
 * @param <K> output key type of the delegate
 * @param <V> output value type of the delegate
 */
public abstract class DelegatingOutputFormat<K, V> extends OutputFormat<NullWritable, StructuredRecord> {
  private OutputFormat<K, V> delegate;

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    RecordWriter<K, V> delegateWriter = getDelegate().getRecordWriter(context);
    return new DelegatingRecordWriter<>(delegateWriter, getConversion(context));
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    getDelegate().checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return getDelegate().getOutputCommitter(context);
  }

  private OutputFormat<K, V> getDelegate() throws IOException, InterruptedException {
    if (delegate == null) {
      delegate = createDelegate();
    }
    return delegate;
  }

  protected abstract OutputFormat<K, V> createDelegate() throws IOException, InterruptedException;

  protected abstract Function<StructuredRecord, KeyValue<K, V>> getConversion(TaskAttemptContext context)
    throws IOException;
}
