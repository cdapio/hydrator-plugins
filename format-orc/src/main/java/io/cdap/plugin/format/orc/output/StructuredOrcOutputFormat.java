/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.format.orc.output;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.plugin.format.orc.StructuredToOrcTransformer;
import io.cdap.plugin.format.output.DelegatingOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

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
