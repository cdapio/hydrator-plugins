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

package co.cask.format.parquet;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.hydrator.format.StructuredToAvroTransformer;
import co.cask.hydrator.format.output.DelegatingOutputFormat;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.function.Function;

/**
 * Converts StructuredRecord into GenericRecord before delegating to AvroKeyOutputFormat.
 */
public class StructuredParquetOutputFormat extends DelegatingOutputFormat<Void, GenericRecord> {

  @Override
  protected OutputFormat<Void, GenericRecord> createDelegate() {
    return new AvroParquetOutputFormat<>();
  }

  @Override
  protected Function<StructuredRecord, KeyValue<Void, GenericRecord>> getConversion(TaskAttemptContext context)
    throws IOException {

    Configuration hConf = context.getConfiguration();
    Schema schema = Schema.parseJson(hConf.get(ParquetOutputFormatProvider.SCHEMA_KEY));
    StructuredToAvroTransformer transformer = new StructuredToAvroTransformer(schema);
    return record -> {
      try {
        return new KeyValue<>(null, transformer.transform(record));
      } catch (IOException e) {
        throw new RuntimeException("Unable to transform structured record into a generic record", e);
      }
    };
  }
}
