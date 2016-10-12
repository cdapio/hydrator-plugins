/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.aggregator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Custom mapper class for Order By plugin
 */
public class CompositeKeyMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {

  @Override
  /**
   * @param key Longwritable key received through pipeline
   * @param value StructuredRecord received as a JSON string
   * @param context object containing the configuration properties for the current job
   */
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();
    context.write(new CompositeKey(value, configuration.get("sortFieldList"), configuration.get("schema")), value);
  }
}
