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

package co.cask.format.avro;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.hydrator.format.StructuredToAvroTransformer;
import co.cask.hydrator.format.output.DelegatingOutputFormat;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.function.Function;

/**
 * Converts StructuredRecord into GenericRecord before delegating to AvroKeyOutputFormat.
 */
public class StructuredAvroOutputFormat extends DelegatingOutputFormat<AvroKey<GenericRecord>, NullWritable> {

  @Override
  protected OutputFormat<AvroKey<GenericRecord>, NullWritable> createDelegate() {
    return new AvroKeyOutputFormat<>();
  }

  @Override
  protected Function<StructuredRecord, KeyValue<AvroKey<GenericRecord>, NullWritable>> getConversion(
    TaskAttemptContext context) throws IOException {

    Configuration hConf = context.getConfiguration();
    Schema schema = Schema.parseJson(hConf.get(AvroOutputFormatProvider.SCHEMA_KEY));
    StructuredToAvroTransformer transformer = new StructuredToAvroTransformer(schema);
    return record -> {
      try {
        return new KeyValue<>(new AvroKey<>(transformer.transform(record)), NullWritable.get());
      } catch (IOException e) {
        throw new RuntimeException("Unable to transform structured record into a generic record", e);
      }
    };
  }
}
