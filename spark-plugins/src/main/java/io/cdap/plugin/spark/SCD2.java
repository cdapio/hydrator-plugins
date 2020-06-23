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

import com.google.common.collect.Ordering;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import javax.annotation.Nullable;

@Name("SCD2")
@Plugin(type = SparkCompute.PLUGIN_TYPE)
public class SCD2 extends SparkCompute<StructuredRecord, StructuredRecord> {
  private final Conf conf;

  public SCD2(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
    // todo: validate key field is allowed type, start & end time fields are dates
  }

  @Override
  public void prepareRun(SparkPluginContext context) {
    // todo: validate + field level lineage
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> javaRDD) {
    return javaRDD.mapToPair(new RecordToKeyRecordPair(conf.key, conf.startDateField))
      .repartitionAndSortWithinPartitions(new HashPartitioner(conf.getNumPartitions()), new KeyComparator())
      // records are now sorted by key and start date (desc). ex: r1, r2, r3, r4
      // we need to walk the records in order and update the end time of r2 to be start time of r1 - 1.
      .map(new SCD2Map(conf.startDateField, conf.endDateField));
  }

  public static class KeyComparator implements Comparator<SCD2Key>, Serializable {

    @Override
    public int compare(SCD2Key k1, SCD2Key k2) {
      int cmp = k1.getKey().compareTo(k2.getKey());
      if (cmp != 0) {
        return cmp;
      }
      return 0 - Integer.compare(k1.getStartDate(), k2.getStartDate());
    }
  }

  /**
   * maps a record to a key field plus the record.
   */
  public static class RecordToKeyRecordPair implements PairFunction<StructuredRecord, SCD2Key, StructuredRecord> {
    private final String keyField;
    private final String startDateField;

    public RecordToKeyRecordPair(String keyField, String startDateField) {
      this.keyField = keyField;
      this.startDateField = startDateField;
    }

    @Override
    public Tuple2<SCD2Key, StructuredRecord> call(StructuredRecord record) {
      // TODO: how should null keys be handled? or null start times?
      return new Tuple2<>(new SCD2Key(record.get(keyField), record.get(startDateField)), record);
    }
  }

  public static class SCD2Map implements Function<Tuple2<SCD2Key, StructuredRecord>, StructuredRecord> {
    private static final LocalDate ACTIVE_DATE = LocalDate.of(9999, 12, 31);
    private final String startDateField;
    private final String endDateField;
    private transient SCD2Key prevKey;
    private transient LocalDate prevStartDate;

    public SCD2Map(String startDateField, String endDateField) {
      this.startDateField = startDateField;
      this.endDateField = endDateField;
    }

    @Override
    public StructuredRecord call(Tuple2<SCD2Key, StructuredRecord> tuple) {
      StructuredRecord record = tuple._2();
      StructuredRecord.Builder builder = StructuredRecord.builder(record.getSchema());
      for (Schema.Field field : record.getSchema().getFields()) {
        builder.set(field.getName(), record.get(field.getName()));
      }

      LocalDate endDate;
      if (!tuple._1().equals(prevKey)) {
        endDate = ACTIVE_DATE;
      } else {
        endDate = prevStartDate.minus(1, ChronoUnit.DAYS);
      }

      builder.setDate(endDateField, endDate);

      // TODO: handle nulls in end date?
      // TODO: skip record if it's a dupe
      prevStartDate = record.getDate(startDateField);
      prevKey = tuple._1();
      return builder.build();
    }
  }

  private static class Conf extends PluginConfig {
    private String key;

    private String startDateField;

    private String endDateField;

    @Nullable
    private Integer numPartitions;

    private int getNumPartitions() {
      return numPartitions == null ? 200 : numPartitions;
    }
  }
}
