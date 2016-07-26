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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * Main driver for the Order By MapReduce program.
 */
public class CompositeKeyDriver {
  public static void main(String[] args) throws Exception {
    Job job = Job.getInstance(new Configuration(), "Order By");
    job.setJarByClass(CompositeKeyDriver.class);

    job.setMapperClass(CompositeKeyMapper.class);
    job.setMapOutputKeyClass(CompositeKey.class);
    job.setMapOutputValueClass(Text.class);

    job.setPartitionerClass(CompositeKeyPartitioner.class);
    job.setGroupingComparatorClass(CompositeKeyGroupComparator.class);
    job.setSortComparatorClass(CompositeKeyComparator.class);

    job.setReducerClass(CompositeKeyReducer.class);
    job.setOutputKeyClass(CompositeKey.class);
    job.setOutputKeyClass(Text.class);

    job.setNumReduceTasks(2);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
