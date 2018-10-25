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

package co.cask.format.delimited;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.format.output.DelegatingOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * Converts StructuredRecord into string before delegating to TextOutputFormat.
 */
public class StructuredDelimitedOutputFormat extends DelegatingOutputFormat<NullWritable, Text> {
  static final String DELIMITER_KEY = "delimiter";

  static Map<String, String> getConfiguration(String delimiter) {
    // base64 encode the delimiter to deal with some common delimiters that are illegal XML characters.
    // most control characters fall into this category.
    // trying to set it in the Hadoop conf will cause parse errors
    String encoded = Base64.getEncoder().encodeToString(delimiter.getBytes(StandardCharsets.UTF_8));
    return Collections.singletonMap(DELIMITER_KEY, encoded);
  }

  @Override
  protected OutputFormat<NullWritable, Text> createDelegate() {
    return new TextOutputFormat<>();
  }

  @Override
  protected Function<StructuredRecord, KeyValue<NullWritable, Text>> getConversion(TaskAttemptContext context) {
    Configuration hConf = context.getConfiguration();
    String encodedDelimiter = hConf.get(DELIMITER_KEY);
    String delimiter = new String(Base64.getDecoder().decode(encodedDelimiter), StandardCharsets.UTF_8);
    return record -> new KeyValue<>(NullWritable.get(),
                                    new Text(StructuredRecordStringConverter.toDelimitedString(record, delimiter)));
  }

}
