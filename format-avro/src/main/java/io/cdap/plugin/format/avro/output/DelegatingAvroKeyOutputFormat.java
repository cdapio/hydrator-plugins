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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroOutputFormatBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Function;

/**
 * Output format for Avro files which uses the DelegatingAvroKeyRecordWriter to write records.
 */
public class DelegatingAvroKeyOutputFormat extends AvroOutputFormatBase<AvroKey<GenericRecord>, NullWritable> {

  @Override
  public RecordWriter<AvroKey<GenericRecord>, NullWritable> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    Function<TaskAttemptContext, OutputStream> outputStreamSupplier = (ctx) -> {
      try {
        return getAvroFileOutputStream(ctx);
      } catch (IOException e) {
        throw new RuntimeException("Unable to get Outpus Stream", e);
      }
    };

    return new DelegatingAvroKeyRecordWriter(context,
                                             getCompressionCodec(context),
                                             outputStreamSupplier,
                                             getSyncInterval(context));
  }
}
