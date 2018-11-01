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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.function.Function;

/**
 * A record writer that transforms a StructuredRecord into some other object and delegates the actual write.
 *
 * @param <K> output key type of the delegate
 * @param <V> output value type of the delegate
 */
public class DelegatingRecordWriter<K, V> extends RecordWriter<NullWritable, StructuredRecord> {
  private final RecordWriter<K, V> delegate;
  private final Function<StructuredRecord, KeyValue<K, V>> conversion;

  public DelegatingRecordWriter(RecordWriter<K, V> delegate, Function<StructuredRecord, KeyValue<K, V>> conversion) {
    this.delegate = delegate;
    this.conversion = conversion;
  }

  @Override
  public void write(NullWritable key, StructuredRecord value) throws IOException, InterruptedException {
    KeyValue<K, V> converted = conversion.apply(value);
    delegate.write(converted.getKey(), converted.getValue());
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    delegate.close(context);
  }
}
