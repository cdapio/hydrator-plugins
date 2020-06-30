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

import com.google.common.base.Splitter;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

@Name("SCD2")
@Plugin(type = SparkCompute.PLUGIN_TYPE)
public class SCD2 extends SparkCompute<StructuredRecord, StructuredRecord> {
  private final Conf config;
  private Schema outputSchema;
  private Set<String> blacklist;

  public SCD2(Conf conf) {
    this.config = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
//    // todo: validate key field is allowed type, start & end time fields are dates
//    // any static configuration validation should happen here.
//    // We will check that the field is in the input schema and is of type string.
//    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
//
//    if (inputSchema == null) {
//      return;
//    }
//
//    // Validate field types
//    if (inputSchema.getField(config.key) == null && !config.containsMacro("id")) {
//      throw new IllegalArgumentException(String.format("Missing id field '%s'", config.key));
//    }
//
//    Schema.Field fromField = inputSchema.getField(config.startDateField);
//    if (!config.containsMacro("effectiveFrom")  && fromField == null) {
//      throw new IllegalArgumentException(String.format("Missing from timestamp field '%s'", config.startDateField));
//    }
//
//    if (!config.containsMacro("effectiveFrom") && fromField != null) {
//      Schema fromSchema = fromField.getSchema();
//      if (fromSchema.isNullable()) {
//        fromSchema = fromSchema.getNonNullable();
//      }
//      if (fromSchema.getType() != Schema.Type.LONG) {
//        throw new IllegalArgumentException(
//          String.format("Field timestamp '%s' is not of underlying type long", config.startDateField));
//      }
//    }
//
//    if (!config.containsMacro("hybridSCD2") && config.hybridSCD2 &&
//          !config.containsMacro("placeHolderFields") && config.placeHolderFields == null) {
//      throw new IllegalArgumentException("Missing placeHolderFields when running hybridSCD2 feature.");
//    }
//
//    if  (config.schema == null ) {
//      // Create output Schema
//      List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
//
//      if (!config.containsMacro("effectiveTo")) {
//
//        // Add same time for effective to timestamp -- must be long based
//        Schema fromTimestamp;
//        if (!config.containsMacro("effectiveFrom") && !config.containsMacro("effectiveTo")) {
//          fromTimestamp = inputSchema.getField(config.startDateField).getSchema();
//          if (!fromTimestamp.isNullable()) {
//            fromTimestamp = Schema.nullableOf(fromTimestamp);
//          }
//          fields.add(Schema.Field.of(config.endDateField, fromTimestamp));
//        } else if (!config.containsMacro("effectiveTo")) {
//          fromTimestamp = Schema.of(Schema.LogicalType.DATE);
//          fields.add(Schema.Field.of(config.endDateField, fromTimestamp));
//        }
//      }
//
//      // Set output schema ( in case effectiveTo is a macro add all beside it)
//      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.recordOf(inputSchema.getRecordName(), fields));
//    }
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws IOException {
//    String effectiveToExistsInSchema;
//
//    try {
//      effectiveToExistsInSchema =  outputSchema.getField(config.endDateField).getName();
//    } catch (Exception e) {
//      effectiveToExistsInSchema = null;
//    }
//
//    if (config.schema != null) {
//      outputSchema = Schema.parseJson(config.schema);
//    } else {
//      outputSchema = context.getOutputSchema();
//    }
//
//    if (!config.containsMacro("effectiveTo") || effectiveToExistsInSchema == null) {
//      Schema toTimestamp;
//      List<Schema.Field> fields = new ArrayList<>();
//      fields.addAll(outputSchema.getFields());
//      toTimestamp = outputSchema.getField(config.startDateField).getSchema();
//      fields.add(Schema.Field.of(config.endDateField, toTimestamp));
//      outputSchema = Schema.recordOf(outputSchema.getRecordName(), fields);
//    }
//
//    //
//    // Record all operations for lineage tracking
//    //
//    ArrayList<FieldOperation> ops = new ArrayList<FieldOperation>();
//
//    // Fill in basic transformations
//    ops.add(new FieldTransformOperation(
//      config.key + " SCD2",
//      "copy",
//      Collections.singletonList(config.key),
//      config.key));
//    ops.add(new FieldTransformOperation(
//      config.startDateField + " SCD2",
//      "copy",
//      Collections.singletonList(config.startDateField),
//      config.startDateField));
//    ops.add(new FieldTransformOperation(
//      config.key + " SCD2",
//      "retrieve from next in SCD2 set or null",
//      Collections.singletonList(config.startDateField),
//      config.endDateField));
//
//    // Description of the general transformation
//    String desc = "copy";
//    if (config.fillInNull) {
//      desc = desc + ", fill from previous if empty";
//    }
//    if (config.deduplicate) {
//      desc = desc + ", remove duplicate rows";
//    }
//
//    if (config.hybridSCD2) {
//      desc = desc + ", Use SCD2 Hybrid Feature";
//      if (config.placeHolderFields != null) {
//        desc = desc + " PlaceHolder Fields";
//      }
//    }
//
//    if (config.blacklist != null) {
//      desc = desc + " blacklist ";
//    }
//
//    // Fill in general transforms
//    for (Schema.Field ofield : outputSchema.getFields()) {
//      String fname = ofield.getName();
//      if (fname.equals(config.startDateField) || fname.equals(config.endDateField) || fname.equals(config.key)) {
//        continue;
//      }
//      ops.add(new FieldTransformOperation(fname + " SCD2", desc, Collections.singletonList(fname), fname));
//    }
//    // Record the lineage
//    context.record(ops);
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
//    if (config.schema != null) {
//      outputSchema = Schema.parseJson(config.schema);
//    } else {
//      outputSchema = context.getOutputSchema();
//    }
//
    blacklist = config.getBlacklist();
//    if (outputSchema == null) {
//      return;
//    }
//
//    String effectiveToExistsInSchema = outputSchema.getField(config.endDateField).getName();
//
//    if (effectiveToExistsInSchema == null) {
//      Schema toTimestamp;
//      List<Schema.Field> fields = new ArrayList<>();
//      fields.addAll(outputSchema.getFields());
//      toTimestamp = outputSchema.getField(config.startDateField).getSchema();
//      fields.add(Schema.Field.of(config.endDateField, toTimestamp));
//      outputSchema = Schema.recordOf(outputSchema.getRecordName(), fields);
//    }
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> javaRDD) {
    JavaRDD<SCD2Record> javaRdd =
      javaRDD.mapToPair(new RecordToKeyRecordPair(config.key, config.startDateField))
        .repartitionAndSortWithinPartitions(new HashPartitioner(config.getNumPartitions()), new KeyComparator())
        .mapPartitions(new FlatMapFunction<Iterator<Tuple2<SCD2Key, StructuredRecord>>, Object>() {

          @Override
          public Iterator<Object> call(Iterator<Tuple2<SCD2Key, StructuredRecord>> tuple2Iterator) throws Exception {
            return null;
          }
        })
        // records are now sorted by key and start date (asc)
        .flatMap(new RecordMap(config.startDateField, config.deduplicate, blacklist));
    return javaRdd.flatMap(new DateMap(config.startDateField, config.endDateField));
  }

  public static class KeyComparator implements Comparator<SCD2Key>, Serializable {

    @Override
    public int compare(SCD2Key k1, SCD2Key k2) {
      int cmp = k1.getKey().compareTo(k2.getKey());
      if (cmp != 0) {
        return cmp;
      }
      return Integer.compare(k1.getStartDate(), k2.getStartDate());
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

  public static class RecordMap implements FlatMapFunction<Tuple2<SCD2Key, StructuredRecord>, SCD2Record> {
    private final String startDateField;
    private final boolean deduplicate;
    private final Set<String> blacklist;
    private transient SCD2Key prevKey;
    private transient AtomicInteger prevStartDate;
    private transient StructuredRecord prevRecord;

    public RecordMap(String startDateField, boolean deduplicate, Set<String> blacklist) {
      this.startDateField = startDateField;
      this.deduplicate = deduplicate;
      this.blacklist = blacklist;
    }

    @Override
    public Iterable<SCD2Record> call(Tuple2<SCD2Key, StructuredRecord> tuple) {
      SCD2Key key = tuple._1();
      StructuredRecord record = tuple._2();
      // TODO: handle nulls in start date? Or simply restrict the schema to be non-nullable
      Integer date = record.get(startDateField);
      // if the current key not equal to previous key, this is a new record, so it doesn't have previous key
      if (!key.equals(this.prevKey)) {
        this.prevKey = key;
        prevRecord = record;
        prevStartDate = new AtomicInteger(date);
        return Collections.singletonList(new SCD2Record(null, record, prevStartDate));
      }

      boolean isDiff = false;
      for (Schema.Field field : record.getSchema().getFields()) {
        String fieldName = field.getName();
        Object value = record.get(fieldName);
        if (blacklist.contains(fieldName) || isDiff) {
          continue;
        }

        // check if there is difference between prev record and cur record
        Object prevVal = prevRecord.get(fieldName);
        if ((prevVal == null) != (value == null) || (value != null && !value.equals(prevVal))) {
          isDiff = true;
        }
      }

      // if there is no difference, skip this record and return empty list
      if (deduplicate && !isDiff) {
        return Collections.emptyList();
      }

      this.prevKey = key;
      prevRecord = record;
      // set the date so all the previous references to this date are updated
      prevStartDate.set(date);
      return Collections.singletonList(new SCD2Record(prevRecord, record, prevStartDate));
    }
  }

  public static class DateMap implements FlatMapFunction<SCD2Record, StructuredRecord> {
    private static final LocalDate ACTIVE_DATE = LocalDate.of(9999, 12, 31);
    private final String startDateField;
    private final String endDateField;

    public DateMap(String startDateField, String endDateField) {
      this.startDateField = startDateField;
      this.endDateField = endDateField;
    }

    @Override
    public Iterable<StructuredRecord> call(SCD2Record record) throws Exception {
      StructuredRecord cur = record.getCur();
      Integer date = cur.get(startDateField);
      StructuredRecord prev = record.getPrev();
      int latestDate = record.getLatestDate().get();

      // if the prev is null and the cur record date is not the latest, return an empty list since we are not able to
      // calculate the end date based on the known information
      if (prev == null && !(latestDate == date)) {
        return Collections.emptyList();
      }

      List<StructuredRecord> result = new ArrayList<>();
      // if prev is null, this means this current record is the only one with this key, so mark it as active
      // otherwise we process the previous record since we now have the start date to compute the end date of it
      StructuredRecord processingRecord = prev == null ? cur : prev;
      StructuredRecord.Builder builder = buildRecord(processingRecord);

      LocalDate endDate;
      if (prev == null) {
        endDate = ACTIVE_DATE;
      } else {
        endDate = LocalDate.ofEpochDay(date).minus(1, ChronoUnit.DAYS);
      }

      builder.setDate(endDateField, endDate);
      result.add(builder.build());

      // if the latest date matches, this is the last record, so mark it as active and add it to result
      if (prev != null && latestDate == date) {
        builder = buildRecord(cur);
        builder.setDate(endDateField, ACTIVE_DATE);
        result.add(builder.build());
      }
      return result;
    }

    private StructuredRecord.Builder buildRecord(StructuredRecord processingRecord) {
      StructuredRecord.Builder builder = StructuredRecord.builder(processingRecord.getSchema());
      for (Schema.Field field : processingRecord.getSchema().getFields()) {
        String fieldName = field.getName();
        Object value = processingRecord.get(fieldName);
        builder.set(fieldName, value);
      }
      return builder;
    }
  }

//  public static class SCD2Map implements Function<Tuple2<SCD2Key, StructuredRecord>,
//                                                   Tuple3<StructuredRecord, StructuredRecord, AtomicInteger>> {
//    private static final LocalDate ACTIVE_DATE = LocalDate.of(9999, 12, 31);
//    private final String startDateField;
//    private final String endDateField;
//    private final boolean deduplicate;
//    private final Set<String> blacklist;
//    private transient SCD2Key prevKey;
//    private transient LocalDate prevStartDate;
//    private transient StructuredRecord prevRecord;
//
//    public SCD2Map(String startDateField, String endDateField, boolean deduplicate, Set<String> blacklist) {
//      this.startDateField = startDateField;
//      this.endDateField = endDateField;
//      this.deduplicate = deduplicate;
//      this.blacklist = blacklist;
//    }
//
//    @Override
//    public StructuredRecord call(Tuple2<SCD2Key, StructuredRecord> tuple) {
//      StructuredRecord record = tuple._2();

//
//      // if there is no difference, return null
//      if (deduplicate && !tuple._1().equals(prevKey) && !isDiff) {
//        return null;
//      }
//

//      endDate.
//
//      // TODO: handle nulls in end date?
//      // TODO: skip record if it's a dupe
//      prevStartDate = record.getDate(startDateField);
//      prevKey = tuple._1();
//      prevRecord = tuple._2();
//      return builder.build();
//    }
//  }

  private static class Conf extends PluginConfig {

    @Macro
    @Description("ID field for unique records - must be string")
    private String key;

    @Macro
    @Description("Effective from date timestamp")
    private String startDateField;

    @Macro
    @Description("Effective to date timestamp")
    private String endDateField;

    @Macro
    @Description("Deduplicate records that have no changes")
    private Boolean deduplicate;

//    @Macro
//    @Description("Fill in null fields from previous")
//    private Boolean fillInNull;

//    @Macro
//    @Description("Use SCD2 Hybrid Feature")
//    private Boolean hybridSCD2;

    @Nullable
    @Macro
    @Description("Blacklist for fields to ignore")
    private String blacklist;

    // A nullable fields tells CDAP that this is an optional field.
//    @Nullable
//    @Macro
//    @Description("PlaceHolder Fields")
//    private String placeHolderFields;

//    @Name("schema")
//    @Macro
//    @Description("Specifies the schema of the records outputted from this plugin.")
//    private String schema;

    @Nullable
    private Integer numPartitions;

    private int getNumPartitions() {
      return numPartitions == null ? 200 : numPartitions;
    }

    Set<String> getBlacklist() {
      Set<String> fields = new HashSet<>();
      if (containsMacro("blacklist") || blacklist == null) {
        return fields;
      }
      for (String field : Splitter.on(',').trimResults().split(blacklist)) {
        fields.add(field);
      }
      return fields;
    }
  }
}
