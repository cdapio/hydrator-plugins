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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 */
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
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    conf.validate(stageConfigurer.getInputSchema(), failureCollector);
    failureCollector.getOrThrowException();
    stageConfigurer.setOutputSchema(conf.getOutputSchema(stageConfigurer.getInputSchema()));
  }

  @Override
  public void prepareRun(SparkPluginContext context) {
    conf.validate(context.getInputSchema(), context.getFailureCollector());
    List<FieldOperation> ops = new ArrayList<FieldOperation>();

    // Fill in basic transformations
    ops.add(new FieldTransformOperation(conf.key + " SCD2", "copy", Collections.singletonList(conf.key), conf.key));
    ops.add(new FieldTransformOperation(
      conf.startDateField + " SCD2", "copy", Collections.singletonList(conf.startDateField),
      conf.startDateField));
    ops.add(new FieldTransformOperation(
      conf.endDateField + " SCD2", "Computed end date field from the start date field",
      Collections.singletonList(conf.startDateField), conf.endDateField));

    // Description of the general transformation
    String desc = "copy";
    if (conf.fillInNull()) {
      desc = desc + ", fill from previous if empty";
    }
    if (conf.deduplicate()) {
      desc = desc + ", remove duplicate rows";
    }
    if (conf.enableHybridSCD2()) {
      desc = desc + ", use hybrid scd2 feature";
    }

    Schema outputSchema = conf.getOutputSchema(context.getInputSchema());
    if (outputSchema == null) {
      context.record(ops);
      return;
    }

    // Fill in general transforms
    for (Schema.Field field : outputSchema.getFields()) {
      String fname = field.getName();
      if (fname.equals(conf.startDateField) || fname.equals(conf.endDateField) || fname.equals(conf.key)) {
        continue;
      }
      ops.add(new FieldTransformOperation(fname + " SCD2", desc, Collections.singletonList(fname), fname));
    }
    // Record the lineage
    context.record(ops);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> javaRDD) {
    return javaRDD.mapToPair(new RecordToKeyRecordPair(conf.key, conf.startDateField))
      .repartitionAndSortWithinPartitions(new HashPartitioner(conf.getNumPartitions()), new KeyComparator())
      // records are now sorted by key and start date (desc). ex: r1, r2, r3, r4
      // we need to walk the records in order and update the end time of r2 to be start time of r1 - 1.
      .mapPartitions(new SCD2FlapMap(conf))
      .filter((Function<StructuredRecord, Boolean>) Objects::nonNull);
  }

  /**
   * Compare the scd2key, first compare the key and then compare the start date.
   */
  public static class KeyComparator implements Comparator<SCD2Key>, Serializable {

    @Override
    public int compare(SCD2Key k1, SCD2Key k2) {
      int cmp = k1.getKey().compareTo(k2.getKey());
      if (cmp != 0) {
        return cmp;
      }
      return Long.compare(k1.getStartDate(), k2.getStartDate());
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
      Object key = record.get(keyField);
      if (key == null) {
        throw new IllegalArgumentException("The key should not be null");
      }
      return new Tuple2<>(new SCD2Key((Comparable) key, record.get(startDateField)), record);
    }
  }

  /**
   *
   */
  public static class SCD2FlapMap
    implements FlatMapFunction<Iterator<Tuple2<SCD2Key, StructuredRecord>>, StructuredRecord> {
    private final Conf conf;

    public SCD2FlapMap(Conf conf) {
      this.conf = conf;

    }

    @Override
    public Iterator<StructuredRecord> call(Iterator<Tuple2<SCD2Key, StructuredRecord>> records) {
      return new SCD2Iterator(records, conf);
    }
  }

  /**
   * The scd2 iterator, it keeps track of cur, prev, next from the given iterator.
   */
  public static class SCD2Iterator extends AbstractIterator<StructuredRecord> {
    // 9999-12-31 00:00:00 timestamp in micro seconds
    private static final long ACTIVE_TS = 253402214400000000L;
    private final Iterator<Tuple2<SCD2Key, StructuredRecord>> records;
    private final Table<Object, String, Object> valTable;
    private final Conf conf;
    private final Set<String> blacklist;
    private final Set<String> placeholders;
    private Schema outputSchema;
    private Tuple2<SCD2Key, StructuredRecord> cur;
    private Tuple2<SCD2Key, StructuredRecord> prev;
    private Tuple2<SCD2Key, StructuredRecord> next;

    public SCD2Iterator(Iterator<Tuple2<SCD2Key, StructuredRecord>> records, Conf conf) {
      this.records = records;
      this.conf = conf;
      this.blacklist = conf.getBlacklist();
      this.valTable = HashBasedTable.create();
      this.placeholders = conf.getPlaceHolderFields();
    }

    @Override
    protected StructuredRecord computeNext() {
      // if the records does not have value, but next still have a value, we still need to process it
      if (!records.hasNext() && next == null) {
        return endOfData();
      }

      prev = cur;
      cur = next != null ? next : records.next();
      next = records.hasNext() ? records.next() : null;

      // deduplicate the result
      if (conf.deduplicate() && next != null && next._1().equals(cur._1())) {
        boolean isDiff = false;
        for (Schema.Field field : cur._2().getSchema().getFields()) {
          String fieldName = field.getName();
          Object value = cur._2().get(fieldName);
          if (blacklist.contains(fieldName)) {
            continue;
          }

          // check if there is difference between next record and cur record
          Object nextVal = next._2().get(fieldName);
          if ((nextVal == null) != (value == null) || (value != null && !value.equals(nextVal))) {
            isDiff = true;
            break;
          }
        }
        if (!isDiff) {
          return null;
        }
      }

      // if key changes, clean up the table to free memory
      if (prev != null && !prev._1().equals(cur._1())) {
        valTable.row(prev._1().getKey()).clear();
      }

      return computeRecord(cur._1().getKey(),
                           prev != null && prev._1().equals(cur._1()) ? prev._2() : null,
                           cur._2(),
                           next != null && next._1().equals(cur._1()) ? next._2() : null);
    }

    private StructuredRecord computeRecord(Object key, @Nullable StructuredRecord prev, StructuredRecord cur,
                                           @Nullable StructuredRecord next) {
      if (outputSchema == null) {
        outputSchema = conf.getOutputSchema(cur.getSchema());
      }

      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

      for (Schema.Field field : cur.getSchema().getFields()) {
        String fieldName = field.getName();
        Object value = cur.get(fieldName);
        // if enable scd2 hybrid, check for prev record is a late arriving data which does not have an end date,
        // also make sure the current record is either closed or is not late arriving date which has a end date
        if (conf.enableHybridSCD2() && placeholders.contains(fieldName) && prev != null &&
              prev.get(conf.endDateField) == null && (next == null || cur.get(conf.endDateField) != null)) {
          value = prev.get(fieldName);
        }

        // fill in null from previous record
        if (conf.fillInNull() && value == null) {
          value = valTable.get(key, fieldName);
        }
        builder.set(fieldName, value);
        if (conf.fillInNull() && value != null) {
          valTable.put(key, fieldName, value);
        }
      }

      long endDate;
      if (next == null) {
        endDate = ACTIVE_TS;
      } else {
        Long date = next.get(conf.startDateField);
        // TODO: handle nulls in start date? Or simply restrict the schema to be non-nullable
        endDate = date == null ? ACTIVE_TS : date - 1000000L;
      }
      builder.set(conf.endDateField, endDate);
      return builder.build();
    }
  }

  /**
   * Conf for scd2 plugin
   */
  @SuppressWarnings({"unused", "ConstantConditions"})
  public static class Conf extends PluginConfig {
    private static final String KEY = "key";
    private static final String START_DATE_FIELD = "startDateField";
    private static final String END_DATE_FIELD = "endDateField";
    private static final String SCD2 = "hybridSCD2";
    private static final String PLACEHOLDER = "placeHolderFields";
    private static final String BLACKLIST = "blacklist";

    @Macro
    @Description("The name of the key field. The records will be grouped based on the key. " +
                   "This field must be comparable.")
    private String key;

    @Macro
    @Description("The name of the start date field. The grouped records are sorted based on this field.")
    private String startDateField;

    @Macro
    @Description("The name of the end date field. The sorted results are iterated to compute the value of this " +
                   "field basedon the start date.")
    private String endDateField;

    @Nullable
    @Macro
    @Description("Deduplicate records that have no changes.")
    private Boolean deduplicate;

    @Nullable
    @Macro
    @Description("Fill in null fields from most recent previous record.")
    private Boolean fillInNull;

    @Nullable
    @Macro
    @Description("Blacklist for fields to ignore to compare when deduplicating the record.")
    private String blacklist;

    @Nullable
    @Macro
    @Description("Use SCD2 Hybrid Feature.")
    private Boolean hybridSCD2;

    @Nullable
    @Macro
    @Description("PlaceHolder fields when using SCD2 Hybrid feature.")
    private String placeHolderFields;

    @Nullable
    @Macro
    @Description("Number of partitions to use when grouping fields. If not specified, the execution" +
                   "framework will decide on the number to use.")
    private Integer numPartitions;

    @VisibleForTesting
    public Conf(String key, String startDateField, String endDateField, boolean deduplicate,
                boolean fillInNull, String blacklist, boolean hybridSCD2, String placeHolderFields) {
      this.key = key;
      this.startDateField = startDateField;
      this.endDateField = endDateField;
      this.deduplicate = deduplicate;
      this.fillInNull = fillInNull;
      this.blacklist = blacklist;
      this.hybridSCD2 = hybridSCD2;
      this.placeHolderFields = placeHolderFields;
    }

    private boolean deduplicate() {
      return deduplicate;
    }

    private boolean fillInNull() {
      return fillInNull;
    }

    public boolean enableHybridSCD2() {
      return hybridSCD2 == null ? false : hybridSCD2;
    }

    private int getNumPartitions() {
      return numPartitions == null ? 200 : numPartitions;
    }

    public Set<String> getPlaceHolderFields() {
      return getFields(PLACEHOLDER, placeHolderFields);
    }

    private Set<String> getBlacklist() {
      return getFields(BLACKLIST, blacklist);
    }

    private Set<String> getFields(String fieldName, String fieldString) {
      Set<String> fields = new HashSet<>();
      if (containsMacro(fieldName) || fieldString == null) {
        return fields;
      }
      for (String field : Splitter.on(',').trimResults().split(fieldString)) {
        fields.add(field);
      }
      return fields;
    }

    private void validate(@Nullable Schema actualSchema, FailureCollector failureCollector) {
      if (actualSchema == null) {
        return;
      }

      if (!containsMacro(KEY)) {
        Schema.Field field = actualSchema.getField(key);
        if (field == null) {
          failureCollector.addFailure(String.format("The %s field '%s' does not exist in input schema.", KEY, key),
                                      null).withConfigElement(KEY, key);
        } else {
          Schema schema = field.getSchema();
          Schema.Type fieldType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
          if (!schema.isSimpleOrNullableSimple()) {
            failureCollector.addFailure(String.format("The %s field '%s' must be a comparable type in " +
                                                        "the input schema.", KEY, key), null)
              .withConfigElement(KEY, key);
          }
        }
      }

      if (!containsMacro(START_DATE_FIELD) && actualSchema.getField(startDateField) == null) {
        Schema.Field field = actualSchema.getField(startDateField);
        if (field == null) {
          failureCollector.addFailure(String.format("The %s field '%s' does not exist in input schema.",
                                                    START_DATE_FIELD, startDateField),
                                      null).withConfigElement(START_DATE_FIELD, startDateField);
        } else {
          Schema schema = field.getSchema();
          Schema.Type fieldType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
          if (!Schema.Type.LONG.equals(fieldType)) {
            failureCollector.addFailure(String.format("The %s field '%s' is not timestamp type in " +
                                                        "the input schema.", START_DATE_FIELD, startDateField), null)
              .withConfigElement(START_DATE_FIELD, startDateField);
          }
        }
      }

      if (!containsMacro(END_DATE_FIELD)) {
        Schema.Field field = actualSchema.getField(endDateField);
        if (field != null) {
          Schema schema = field.getSchema();
          Schema.Type fieldType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
          if (!Schema.Type.LONG.equals(fieldType)) {
            failureCollector.addFailure(String.format("The %s field '%s' is not timestamp type type in " +
                                                        "the input schema.", END_DATE_FIELD, endDateField), null)
              .withConfigElement(END_DATE_FIELD, endDateField);
          }
        }
      }

      if (!containsMacro(SCD2) && !containsMacro(PLACEHOLDER) && enableHybridSCD2() && placeHolderFields == null) {
        failureCollector.addFailure(String.format("The %s field must be provided when the " +
                                                    "scd2 hybrid feature is enabled.", PLACEHOLDER), null);
      }
    }

    @Nullable
    private Schema getOutputSchema(@Nullable Schema inputSchema) {
      if (inputSchema == null) {
        return null;
      }

      if (!containsMacro(END_DATE_FIELD) && inputSchema.getField(endDateField) != null) {
        return inputSchema;
      }

      List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
      Schema endSchema = !containsMacro(START_DATE_FIELD) ? inputSchema.getField(startDateField).getSchema() :
                           Schema.of(Schema.LogicalType.DATE);
      fields.add(Schema.Field.of(endDateField, endSchema));
      return Schema.recordOf(inputSchema.getRecordName(), fields);
    }
  }
}
