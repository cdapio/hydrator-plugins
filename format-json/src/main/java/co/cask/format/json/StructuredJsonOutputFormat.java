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

package co.cask.format.json;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.format.output.DelegatingOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.function.Function;

/**
 * Converts StructuredRecord into string before delegating to TextOutputFormat.
 */
public class StructuredJsonOutputFormat extends DelegatingOutputFormat<NullWritable, Text> {

  @Override
  protected OutputFormat<NullWritable, Text> createDelegate() {
    return new TextOutputFormat<>();
  }

  @Override
  protected Function<StructuredRecord, KeyValue<NullWritable, Text>> getConversion(TaskAttemptContext context) {
    return record -> {
      try {
        return new KeyValue<>(NullWritable.get(), new Text(StructuredRecordStringConverter.toJsonString(record)));
      } catch (IOException e) {
        throw new RuntimeException("Unable to convert record into a json object", e);
      }
    };
  }

}
