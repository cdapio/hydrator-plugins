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

package co.cask.format.orc;

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
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;
import java.util.function.Function;

/**
 * Converts StructuredRecord into OrcStruct before delegating to OrcOutputFormat.
 */
public class StructuredOrcOutputFormat extends DelegatingOutputFormat<NullWritable, OrcStruct> {

  @Override
  protected OutputFormat<NullWritable, OrcStruct> createDelegate() {
    return new OrcOutputFormat<>();
  }

  @Override
  protected Function<StructuredRecord, KeyValue<NullWritable, OrcStruct>> getConversion(TaskAttemptContext context) {
    StructuredToOrcTransformer transformer = new StructuredToOrcTransformer();
    return record -> new KeyValue<>(NullWritable.get(), transformer.transform(record));
  }
}
