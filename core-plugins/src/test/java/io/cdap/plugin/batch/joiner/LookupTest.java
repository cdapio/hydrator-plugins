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

package io.cdap.plugin.batch.joiner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.batch.ETLBatchTestBase;
import io.cdap.plugin.common.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * Tests for Lookup.
 */
public class LookupTest extends ETLBatchTestBase {
  private static final String CUSTOMER_DB_NAME = "customer";
  private static final String PHONE_NUMBERS_DB_NAME = "phone_numbers";
  private static final String INPUT_KEY_FIELD = "customer_id";
  private static final String LOOKUP_KEY_FIELD = "customer_id";
  private static final String LOOKUP_FIELD = "phone_number";
  private static final String OUTPUT_FIELD = "phone";
  private static final String CUSTOMER_DATASET_NAME = "customer_dataset";
  private static final String PHONE_NUMBERS_DATASET_NAME = "phone_numbers_dataset";
  private static final String JOINED_DATASET_NAME = "joined_dataset";
  private static final Schema CUSTOMER_SCHEMA = Schema.recordOf(
    CUSTOMER_DB_NAME,
    Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("first_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("last_name", Schema.of(Schema.Type.STRING)));

  private static final Schema PHONE_NUMBERS_SCHEMA = Schema.recordOf(
    PHONE_NUMBERS_DB_NAME,
    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("phone_number", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT_SCHEMA = Schema.recordOf(
    "joined",
    Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("first_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("last_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of(OUTPUT_FIELD, Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  private void joinHelper(ETLStage customerStage, ETLStage phoneNumbersStage, ETLStage joinStage,
                          ETLStage joinSinkStage, String customerDatasetName, String phoneNumberDatasetName,
                          String joinedDatasetName, Schema outputSchema,
                          BiFunction<Schema, TimePartitionedFileSet, Void> verifyOutput,
                          Map<String, String> runTimeProperties) throws Exception {

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(customerStage)
      .addStage(phoneNumbersStage)
      .addStage(joinStage)
      .addStage(joinSinkStage)
      .addConnection(customerStage.getName(), joinStage.getName())
      .addConnection(phoneNumbersStage.getName(), joinStage.getName())
      .addConnection(joinStage.getName(), joinSinkStage.getName())
      .build();
    ApplicationManager appManager = deployETL(config, UUID.randomUUID().toString());

    // ingest data
    ingestToCustomerTable(customerDatasetName);
    ingestToPhoneNumbersTable(phoneNumberDatasetName);

    // run the pipeline
    runETLOnce(appManager, runTimeProperties);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(joinedDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();

    // verify join output
    verifyOutput.apply(outputSchema, fileSet);
  }

  @Test
  public void testLookup() throws Exception {
    ETLStage customerStage = new ETLStage(CUSTOMER_DB_NAME, MockSource.getPlugin(
      CUSTOMER_DATASET_NAME, CUSTOMER_SCHEMA));
    ETLStage phoneNumberStage = new ETLStage(PHONE_NUMBERS_DB_NAME, MockSource.getPlugin(
      PHONE_NUMBERS_DATASET_NAME, PHONE_NUMBERS_SCHEMA));

    final ImmutableMap<String, String> configProperties = new ImmutableMap.Builder<String, String>()
      .put("lookupDataset", PHONE_NUMBERS_DB_NAME)
      .put("inputKeyField", INPUT_KEY_FIELD)
      .put("lookupKeyField", LOOKUP_KEY_FIELD)
      .put("lookupValueField", LOOKUP_FIELD)
      .put("outputField", OUTPUT_FIELD)
      .put("schema", OUTPUT_SCHEMA.toString())
      .build();
    ETLStage joinStage =
      new ETLStage("joiner",
                   new ETLPlugin("Lookup",
                                 Lookup.PLUGIN_TYPE,
                                 configProperties
                   ));
    ETLStage joinSinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, OUTPUT_SCHEMA.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, JOINED_DATASET_NAME),
                            null));
    joinHelper(customerStage, phoneNumberStage, joinStage, joinSinkStage,
               CUSTOMER_DATASET_NAME, PHONE_NUMBERS_DATASET_NAME, JOINED_DATASET_NAME,
               OUTPUT_SCHEMA, this::verifyLookupOutput, ImmutableMap.of());
  }

  private void ingestToPhoneNumbersTable(String phoneNumbersDatasetName) throws Exception {
    DataSetManager<Table> phoneNumbersManager = getDataset(phoneNumbersDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(PHONE_NUMBERS_SCHEMA)
        .set("id", "1")
        .set("customer_id", "1")
        .set("phone_number", "555-555-555")
        .build(),
      StructuredRecord.builder(PHONE_NUMBERS_SCHEMA)
        .set("id", "2")
        .set("customer_id", "1")
        .set("phone_number", "333-333-333")
        .build()
    );
    MockSource.writeInput(phoneNumbersManager, input);
  }

  private void ingestToCustomerTable(String customerDatasetName) throws Exception {
    DataSetManager<Table> customerManager = getDataset(customerDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(CUSTOMER_SCHEMA)
        .set("customer_id", "1")
        .set("first_name", "John")
        .set("last_name", "Doe")
        .build()
    );
    MockSource.writeInput(customerManager, input);
  }

  private GenericRecord getJohnDoeRecord(org.apache.avro.Schema outputSchema) {
    return new GenericRecordBuilder(outputSchema)
      .set("customer_id", "1")
      .set("first_name", "John")
      .set("last_name", "Doe")
      .set("phone", "555-555-555")
      .build();
  }

  private Void verifyLookupOutput(Schema outputSchema, TimePartitionedFileSet fileSet) {
    try {
      Set<GenericRecord> actual = Sets.newHashSet(readOutput(fileSet, outputSchema));
      org.apache.avro.Schema avroOutputSchema = new org.apache.avro.Schema.Parser().parse(outputSchema.toString());
      GenericRecord expected = getJohnDoeRecord(avroOutputSchema);
      Assert.assertEquals(expected, actual.iterator().next());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return null;
  }

}
