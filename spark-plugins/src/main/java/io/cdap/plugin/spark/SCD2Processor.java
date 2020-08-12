/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.spark;

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.Objects;

/**
 *
 */
public final class SCD2Processor {
  private final SCD2.Conf conf;

  public SCD2Processor(SCD2.Conf conf) {
    this.conf = conf;
  }

  JavaRDD<StructuredRecord> process(JavaRDD<StructuredRecord> javaRDD) {
    return javaRDD.mapToPair(new RecordToKeyRecordPairFunction(conf.getKey(), conf.getStartDateField()))
             .repartitionAndSortWithinPartitions(new HashPartitioner(conf.getNumPartitions()), new SCD2.KeyComparator())
             // records are now sorted by key and start date (desc). ex: r1, r2, r3, r4
             // we need to walk the records in order and update the end time of r2 to be start time of r1 - 1.
             .mapPartitions(new SCD2FlatMapFunction(conf))
             .filter((Function<StructuredRecord, Boolean>) Objects::nonNull);
  }
}
