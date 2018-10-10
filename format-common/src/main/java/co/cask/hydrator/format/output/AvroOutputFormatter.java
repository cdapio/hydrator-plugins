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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.hydrator.format.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines logic for reading and writing Avro files.
 */
public class AvroOutputFormatter implements FileOutputFormatter<AvroKey<GenericRecord>, NullWritable> {
  private final StructuredToAvroTransformer recordTransformer;
  private final Schema schema;

  AvroOutputFormatter(Schema schema) {
    recordTransformer = new StructuredToAvroTransformer(schema);
    this.schema = schema;
  }

  @Override
  public KeyValue<AvroKey<GenericRecord>, NullWritable> transform(StructuredRecord record) throws IOException {
    return new KeyValue<>(new AvroKey<>(recordTransformer.transform(record)), NullWritable.get());
  }

  @Override
  public String getFormatClassName() {
    return AvroKeyOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getFormatConfig() {
    Map<String, String> conf = new HashMap<>();
    conf.put("avro.schema.output.key", schema.toString());
    conf.put(JobContext.OUTPUT_KEY_CLASS, AvroKey.class.getName());
    return conf;
  }
}
