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

import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: add
 */
public class DelegatingAvroKeyRecordWriter extends RecordWriter<AvroKey<GenericRecordWrapper>, NullWritable> {
  private final Map<Integer, RecordWriter<AvroKey<GenericRecord>, NullWritable>> delegateMap;
  private final TaskAttemptContext context;
  private final CodecFactory codecFactory;
  private final OutputStream outputStream;
  private final int syncInterval;

  public DelegatingAvroKeyRecordWriter(TaskAttemptContext context,
                                       CodecFactory codecFactory,
                                       OutputStream outputStream,
                                       int syncInterval) {
    super();
    this.delegateMap = new HashMap<>();
    this.context = context;
    this.codecFactory = codecFactory;
    this.outputStream = outputStream;
    this.syncInterval = syncInterval;
  }

  @Override
  public void write(AvroKey<GenericRecordWrapper> key, NullWritable value) throws IOException, InterruptedException {
    RecordWriter<AvroKey<GenericRecord>, NullWritable> delegate =
      delegateMap.computeIfAbsent(key.datum().getSchemaHash(), (k) -> {
        try {
          Configuration conf = context.getConfiguration();
          GenericData dataModel = AvroSerialization.createDataModel(conf);

          return new AvroKeyRecordWriter<>(key.datum().getGenericRecord().getSchema(),
                                           dataModel,
                                           codecFactory,
                                           outputStream,
                                           syncInterval);
        } catch (IOException ioe) {
          throw new RuntimeException("Unable to initialize Avro Record Writer", ioe);
        }
      });

    delegate.write(new AvroKey<>(key.datum().getGenericRecord()), value);
  }

  @Override
  public void close(TaskAttemptContext context)  {
    RuntimeException ex = new RuntimeException("Unable to close delegate.");

    for (RecordWriter<AvroKey<GenericRecord>, NullWritable> delegate : delegateMap.values()) {
      try {
        delegate.close(context);
      } catch (IOException | InterruptedException e) {
        ex.addSuppressed(e);
      }
    }

    if (ex.getSuppressed().length > 0) {
      throw ex;
    }
  }
}
