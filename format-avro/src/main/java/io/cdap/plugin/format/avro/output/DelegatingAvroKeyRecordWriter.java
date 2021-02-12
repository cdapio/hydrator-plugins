/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.format.avro.output;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Record Writer for Avro records that delegates to additional record writers based on the Schema hash.
 */
public class DelegatingAvroKeyRecordWriter extends RecordWriter<AvroKey<GenericRecord>, NullWritable> {
  private final Map<Integer, RecordWriter<AvroKey<GenericRecord>, NullWritable>> delegateMap;
  private final TaskAttemptContext context;
  private final CodecFactory codecFactory;
  private final Function<TaskAttemptContext, OutputStream> outputStreamSupplier;
  private final int syncInterval;

  public DelegatingAvroKeyRecordWriter(TaskAttemptContext context,
                                       CodecFactory codecFactory,
                                       Function<TaskAttemptContext, OutputStream> outputStreamSupplier,
                                       int syncInterval) {
    super();
    this.delegateMap = new HashMap<>();
    this.context = context;
    this.codecFactory = codecFactory;
    this.outputStreamSupplier = outputStreamSupplier;
    this.syncInterval = syncInterval;
  }

  @Override
  public void write(AvroKey<GenericRecord> key, NullWritable value) throws IOException, InterruptedException {
    RecordWriter<AvroKey<GenericRecord>, NullWritable> delegate;
    int schemaHash = key.datum().getSchema().hashCode();

    if (delegateMap.containsKey(schemaHash)) {
      delegate = delegateMap.get(schemaHash);
    } else {
      Configuration conf = context.getConfiguration();
      GenericData dataModel = AvroSerialization.createDataModel(conf);

      delegate = new AvroKeyRecordWriter<>(key.datum().getSchema(),
                                           dataModel,
                                           codecFactory,
                                           outputStreamSupplier.apply(context),
                                           syncInterval);
      delegateMap.put(schemaHash, delegate);
    }

    delegate.write(key, value);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    IOException ex = null;

    for (RecordWriter<AvroKey<GenericRecord>, NullWritable> delegate : delegateMap.values()) {
      try {
        delegate.close(context);
      } catch (IOException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }

    if (ex != null) {
      throw ex;
    }
  }
}
