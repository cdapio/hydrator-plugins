/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
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
 * Tests for Joiner.
 */
public class JoinerTestRun extends ETLBatchTestBase {
  private static final Schema FILM_SCHEMA = Schema.recordOf(
    "film",
    Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)));

  private static final Schema FILM_ACTOR_SCHEMA = Schema.recordOf(
    "filmActor",
    Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("actor_name", Schema.of(Schema.Type.STRING)));

  private static final Schema FILM_CATEGORY_SCHEMA = Schema.recordOf(
    "filmCategory",
    Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("category_name", Schema.of(Schema.Type.STRING)));

  private static final String selectedFields = "film.film_id, film.film_name, filmActor.actor_name as renamed_actor, " +
    "filmCategory.category_name as renamed_category";

  private void joinHelper(ETLStage filmStage, ETLStage filmActorStage, ETLStage filmCategoryStage, ETLStage joinStage,
                          ETLStage joinSinkStage, String filmDatasetName, String filmActorDatasetName,
                          String filmCategoryDatasetName, String joinedDatasetName, Schema outputSchema,
                          BiFunction<Schema, TimePartitionedFileSet, Void> verifyOutput,
                          Map<String, String> runTimeProperties) throws Exception {
    /*
     * film         ---------------
     *                              |
     * filmActor    ---------------   joiner ------- sink
     *                              |
     * filmCategory ---------------
     *
     */

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(filmStage)
      .addStage(filmActorStage)
      .addStage(filmCategoryStage)
      .addStage(joinStage)
      .addStage(joinSinkStage)
      .addConnection(filmStage.getName(), joinStage.getName())
      .addConnection(filmActorStage.getName(), joinStage.getName())
      .addConnection(filmCategoryStage.getName(), joinStage.getName())
      .addConnection(joinStage.getName(), joinSinkStage.getName())
      .build();
    ApplicationManager appManager = deployETL(config, UUID.randomUUID().toString());

    // ingest data
    ingestToFilmTable(filmDatasetName);
    ingestToFilmActorTable(filmActorDatasetName);
    ingestToFilmCategoryTable(filmCategoryDatasetName);

    // run the pipeline
    runETLOnce(appManager, runTimeProperties);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset(joinedDatasetName);
    TimePartitionedFileSet fileSet = outputManager.get();

    // verify join output
    verifyOutput.apply(outputSchema, fileSet);
  }

  @Test
  public void testInnerJoin() throws Exception {
    String filmDatasetName = "film-innerjoin";
    String filmCategoryDatasetName = "film-category-innerjoin";
    String filmActorDatasetName = "film-actor-innerjoin";
    String joinedDatasetName = "innerjoin-output";


    ETLStage filmStage = new ETLStage("film", MockSource.getPlugin(filmDatasetName, FILM_SCHEMA));
    ETLStage filmActorStage = new ETLStage("filmActor", MockSource.getPlugin(filmActorDatasetName, FILM_ACTOR_SCHEMA));
    ETLStage filmCategoryStage = new ETLStage("filmCategory",
                                              MockSource.getPlugin(filmCategoryDatasetName, FILM_CATEGORY_SCHEMA));

    // output schema sorted by input stage names
    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_actor", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_category", Schema.of(Schema.Type.STRING)));

    ETLStage joinStage =
      new ETLStage("joiner",
                   new ETLPlugin("Joiner",
                                 BatchJoiner.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "joinKeys", "film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                     "film.film_name=filmActor.film_name=filmCategory.film_name",
                                   "selectedFields", selectedFields,
                                   "requiredInputs", "film,filmActor,filmCategory"),
                                 null));
    ETLStage joinSinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, joinedDatasetName),
                            null));

    joinHelper(filmStage, filmActorStage, filmCategoryStage, joinStage, joinSinkStage,
               filmDatasetName, filmActorDatasetName, filmCategoryDatasetName, joinedDatasetName,
               outputSchema, this::verifyInnerJoinOutput, ImmutableMap.of());
  }

  @Test
  public void testInnerJoinWithMacro() throws Exception {
    String filmDatasetName = "film-innerjoin-unknown-inputschemas";
    String filmCategoryDatasetName = "film-category-innerjoin-unknown-inputschemas";
    String filmActorDatasetName = "film-actor-innerjoin-unknown-inputschemas";
    String joinedDatasetName = "innerjoin-output-unknown-inputschemas";


    ETLStage filmStage = new ETLStage("film", MockSource.getPlugin(filmDatasetName));
    ETLStage filmActorStage = new ETLStage("filmActor", MockSource.getPlugin(filmActorDatasetName));
    ETLStage filmCategoryStage = new ETLStage("filmCategory", MockSource.getPlugin(filmCategoryDatasetName));

    // output schema sorted by input stage names
    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_actor", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_category", Schema.of(Schema.Type.STRING)));

    Map<String, String> configMap = ImmutableMap.of(
      "joinKeys", "${joinKeys}",
      "selectedFields", "${selectedFields}",
      "requiredInputs", "${requiredInputs}",
      Properties.Table.PROPERTY_SCHEMA, "${" + Properties.Table.PROPERTY_SCHEMA + "}");

    Map<String, String> macroMap = ImmutableMap.of(
      "joinKeys", "film.film_id=filmActor.film_id=filmCategory.film_id&" +
        "film.film_name=filmActor.film_name=filmCategory.film_name",
      "selectedFields", selectedFields,
      "requiredInputs", "film,filmActor,filmCategory",
      Properties.Table.PROPERTY_SCHEMA, outputSchema.toString());

    ETLStage joinStage =
      new ETLStage("joiner",
                   new ETLPlugin("Joiner",
                                 BatchJoiner.PLUGIN_TYPE,
                                 configMap,
                                 null));
    ETLStage joinSinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, joinedDatasetName),
                            null));

    joinHelper(filmStage, filmActorStage, filmCategoryStage, joinStage, joinSinkStage,
               filmDatasetName, filmActorDatasetName, filmCategoryDatasetName, joinedDatasetName,
               outputSchema, this::verifyInnerJoinOutput, macroMap);
  }


  @Test
  public void testOuterJoin() throws Exception {
    String filmDatasetName = "film-outerjoin";
    String filmCategoryDatasetName = "film-category-outerjoin";
    String filmActorDatasetName = "film-actor-outerjoin";
    String joinedDatasetName = "outerjoin-output";

    ETLStage filmStage = new ETLStage("film", MockSource.getPlugin(filmDatasetName, FILM_SCHEMA));
    ETLStage filmActorStage = new ETLStage("filmActor", MockSource.getPlugin(filmActorDatasetName, FILM_ACTOR_SCHEMA));
    ETLStage filmCategoryStage = new ETLStage("filmCategory",
                                              MockSource.getPlugin(filmCategoryDatasetName, FILM_CATEGORY_SCHEMA));

    // output schema sorted by input stage names
    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_actor", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_category", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    ETLStage joinStage =
      new ETLStage("joiner",
                   new ETLPlugin("Joiner",
                                 BatchJoiner.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "joinKeys", "film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                     "film.film_name=filmActor.film_name=filmCategory.film_name",
                                   "selectedFields", selectedFields,
                                   "requiredInputs", "film,filmActor"),
                                 null));

    ETLStage joinSinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, joinedDatasetName),
                            null));

    joinHelper(filmStage, filmActorStage, filmCategoryStage, joinStage, joinSinkStage,
               filmDatasetName, filmActorDatasetName, filmCategoryDatasetName, joinedDatasetName,
               outputSchema, this::verifyOuterJoinOutput, ImmutableMap.of());
  }

  @Test
  public void testOuterJoinWithMacro() throws Exception {
    String filmDatasetName = "film-outerjoin-unknown-inputschemas";
    String filmCategoryDatasetName = "film-category-outerjoin-unknown-inputschemas";
    String filmActorDatasetName = "film-actor-outerjoin-unknown-inputschemas";
    String joinedDatasetName = "outerjoin-output-unknown-inputschemas";

    ETLStage filmStage = new ETLStage("film", MockSource.getPlugin(filmDatasetName, FILM_SCHEMA));
    ETLStage filmActorStage = new ETLStage("filmActor", MockSource.getPlugin(filmActorDatasetName));
    ETLStage filmCategoryStage = new ETLStage("filmCategory",
      MockSource.getPlugin(filmCategoryDatasetName));

    // output schema sorted by input stage names
    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_actor", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_category", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Map<String, String> configMap = ImmutableMap.of(
      "joinKeys", "${joinKeys}",
      "selectedFields", "${selectedFields}",
      "requiredInputs", "${requiredInputs}",
      "numPartitions", "${numPartitions}",
      Properties.Table.PROPERTY_SCHEMA, "${" + Properties.Table.PROPERTY_SCHEMA + "}");
    Map<String, String> macroMap = ImmutableMap.of(
      "joinKeys", "film.film_id=filmActor.film_id=filmCategory.film_id&" +
        "film.film_name=filmActor.film_name=filmCategory.film_name",
      "selectedFields", selectedFields,
      "requiredInputs", "film,filmActor",
      "numPartitions", "2",
      Properties.Table.PROPERTY_SCHEMA, outputSchema.toString());

    ETLStage joinStage =
      new ETLStage("joiner",
        new ETLPlugin("Joiner",
          BatchJoiner.PLUGIN_TYPE,
          configMap,
          null));

    ETLStage joinSinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
        Properties.TimePartitionedFileSetDataset.TPFS_NAME, joinedDatasetName),
      null));

    joinHelper(filmStage, filmActorStage, filmCategoryStage, joinStage, joinSinkStage,
      filmDatasetName, filmActorDatasetName, filmCategoryDatasetName, joinedDatasetName,
      outputSchema, this::verifyOuterJoinOutput, macroMap);
  }

  @Test
  public void testOuterJoinWithoutRequiredInputs() throws Exception {
    String filmDatasetName = "film-outerjoin-no-requiredinputs-unknown-inputschemas";
    String filmCategoryDatasetName = "film-category-outerjoin-no-requiredinputs-unknown-inputschemas";
    String filmActorDatasetName = "film-actor-outerjoin-no-requiredinputs-unknown-inputschemas";
    String joinedDatasetName = "outerjoin-no-requiredinputs-output-unknown-inputschemas";

    ETLStage filmStage = new ETLStage("film", MockSource.getPlugin(filmDatasetName));
    ETLStage filmActorStage = new ETLStage("filmActor", MockSource.getPlugin(filmActorDatasetName));
    ETLStage filmCategoryStage = new ETLStage("filmCategory", MockSource.getPlugin(filmCategoryDatasetName));

    // output schema sorted by input stage names
    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("film_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("film_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("renamed_actor", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("renamed_category", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    ETLStage joinStage =
      new ETLStage("joiner",
                   new ETLPlugin("Joiner",
                                 BatchJoiner.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "joinKeys", "film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                     "film.film_name=filmActor.film_name=filmCategory.film_name",
                                   "selectedFields", selectedFields,
                                   "requiredInputs", "",
                                   "schema", outputSchema.toString()),
                                 null));

    ETLStage joinSinkStage = new ETLStage(
      "sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                            ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, outputSchema.toString(),
                                            Properties.TimePartitionedFileSetDataset.TPFS_NAME, joinedDatasetName),
                            null));

    joinHelper(filmStage, filmActorStage, filmCategoryStage, joinStage, joinSinkStage,
               filmDatasetName, filmActorDatasetName, filmCategoryDatasetName, joinedDatasetName,
               outputSchema, this::verifyOuterJoinWithoutRequiredInputs, ImmutableMap.of());
  }

  private void ingestToFilmCategoryTable(String filmCategoryDatasetName) throws Exception {
    // 1: 1, matrix, action
    // 2: 1, matrix, thriller
    // 3: 2, equilibrium, action
    DataSetManager<Table> filmCategoryManager = getDataset(filmCategoryDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(FILM_CATEGORY_SCHEMA)
        .set("film_id", "1")
        .set("film_name", "matrix")
        .set("category_name", "action")
        .build(),
      StructuredRecord.builder(FILM_CATEGORY_SCHEMA)
        .set("film_id", "1")
        .set("film_name", "matrix")
        .set("category_name", "thriller")
        .build(),
      StructuredRecord.builder(FILM_CATEGORY_SCHEMA)
        .set("film_id", "2")
        .set("film_name", "equilibrium")
        .set("category_name", "action")
        .build(),
      StructuredRecord.builder(FILM_CATEGORY_SCHEMA)
        .set("film_id", "5")
        .set("film_name", "sultan")
        .set("category_name", "comedy")
        .build());
    MockSource.writeInput(filmCategoryManager, input);

  }

  private void ingestToFilmActorTable(String filmActorDatasetName) throws Exception {
    // 1: 1, matrix, alex
    // 2: 1, matrix, bob
    // 3: 2, equilibrium, cathie
    // 4: 3, avatar, samuel
    DataSetManager<Table> filmActorManager = getDataset(filmActorDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(FILM_ACTOR_SCHEMA)
        .set("film_id", "1")
        .set("film_name", "matrix")
        .set("actor_name", "alex")
        .build(),
      StructuredRecord.builder(FILM_ACTOR_SCHEMA)
        .set("film_id", "1")
        .set("film_name", "matrix")
        .set("actor_name", "bob")
        .build(),
      StructuredRecord.builder(FILM_ACTOR_SCHEMA)
        .set("film_id", "2")
        .set("film_name", "equilibrium")
        .set("actor_name", "cathie")
        .build(),
      StructuredRecord.builder(FILM_ACTOR_SCHEMA)
        .set("film_id", "3")
        .set("film_name", "avatar")
        .set("actor_name", "samuel")
        .build());
    MockSource.writeInput(filmActorManager, input);
  }

  private void ingestToFilmTable(String filmDatasetName) throws Exception {
    // write input data
    // 1: 1, matrix
    // 2: 2, equilibrium
    // 3: 3, avatar
    DataSetManager<Table> filmManager = getDataset(filmDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(FILM_SCHEMA)
        .set("film_id", "1")
        .set("film_name", "matrix")
        .build(),
      StructuredRecord.builder(FILM_SCHEMA)
        .set("film_id", "2")
        .set("film_name", "equilibrium")
        .build(),
      StructuredRecord.builder(FILM_SCHEMA)
        .set("film_id", "3")
        .set("film_name", "avatar")
        .build(),
      StructuredRecord.builder(FILM_SCHEMA)
        .set("film_id", "4")
        .set("film_name", "humtum")
        .build());
    MockSource.writeInput(filmManager, input);
  }

  private Void verifyInnerJoinOutput(Schema outputSchema, TimePartitionedFileSet fileSet) {
    try {
      Set<GenericRecord> actual = Sets.newHashSet(readOutput(fileSet, outputSchema));
      Assert.assertEquals(5, actual.size());
      org.apache.avro.Schema avroOutputSchema = new org.apache.avro.Schema.Parser().parse(outputSchema.toString());
      Set<GenericRecord> expected = ImmutableSet.of(getBobRecord1(avroOutputSchema),
                                                    getBobRecord2(avroOutputSchema),
                                                    getAlexRecord1(avroOutputSchema),
                                                    getAlexRecord2(avroOutputSchema),
                                                    getCathieRecord1(avroOutputSchema));
      Assert.assertEquals(expected, actual);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return null;
  }

  private Void verifyOuterJoinOutput(Schema outputSchema, TimePartitionedFileSet fileSet) {
    try {
      Set<GenericRecord> actual = Sets.newHashSet(readOutput(fileSet, outputSchema));
      Assert.assertEquals(6, actual.size());
      org.apache.avro.Schema avroOutputSchema = new org.apache.avro.Schema.Parser().parse(outputSchema.toString());
      Set<GenericRecord> expected = ImmutableSet.of(getBobRecord1(avroOutputSchema),
                                                    getBobRecord2(avroOutputSchema),
                                                    getAlexRecord1(avroOutputSchema),
                                                    getAlexRecord2(avroOutputSchema),
                                                    getCathieRecord1(avroOutputSchema),
                                                    getAvatarRecord1(avroOutputSchema));
      Assert.assertEquals(expected, actual);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return null;
  }

  private Void verifyOuterJoinWithoutRequiredInputs(Schema outputSchema, TimePartitionedFileSet fileSet) {
    try {
      Set<GenericRecord> actual = Sets.newHashSet(readOutput(fileSet, outputSchema));
      Assert.assertEquals(8, actual.size());
      org.apache.avro.Schema avroOutputSchema = new org.apache.avro.Schema.Parser().parse(outputSchema.toString());

      GenericRecord humtumRecord = new GenericRecordBuilder(avroOutputSchema)
        .set("film_id", "4")
        .set("film_name", "humtum")
        .set("renamed_actor", null)
        .set("renamed_category", null)
        .build();

      GenericRecord sultanRecord = new GenericRecordBuilder(avroOutputSchema)
        .set("film_id", null)
        .set("film_name", null)
        .set("renamed_actor", null)
        .set("renamed_category", "comedy")
        .build();

      Set<GenericRecord> expected = ImmutableSet.of(getBobRecord1(avroOutputSchema),
                                                    getBobRecord2(avroOutputSchema),
                                                    getAlexRecord1(avroOutputSchema),
                                                    getAlexRecord2(avroOutputSchema),
                                                    getCathieRecord1(avroOutputSchema),
                                                    getAvatarRecord1(avroOutputSchema),
                                                    humtumRecord, sultanRecord);
      Assert.assertEquals(expected, actual);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return null;
  }

  private GenericRecord getBobRecord1(org.apache.avro.Schema avroOutputSchema) {
    return new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "1")
      .set("film_name", "matrix")
      .set("renamed_category", "thriller")
      .set("renamed_actor", "bob")
      .build();
  }

  private GenericRecord getBobRecord2(org.apache.avro.Schema avroOutputSchema) {
    return new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "1")
      .set("film_name", "matrix")
      .set("renamed_category", "action")
      .set("renamed_actor", "bob")
      .build();
  }

  private GenericRecord getAlexRecord1(org.apache.avro.Schema avroOutputSchema) {
    return new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "1")
      .set("film_name", "matrix")
      .set("renamed_category", "thriller")
      .set("renamed_actor", "alex")
      .build();
  }

  private GenericRecord getAlexRecord2(org.apache.avro.Schema avroOutputSchema) {
    return new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "1")
      .set("film_name", "matrix")
      .set("renamed_category", "action")
      .set("renamed_actor", "alex")
      .build();
  }

  private GenericRecord getCathieRecord1(org.apache.avro.Schema avroOutputSchema) {
    return new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "2")
      .set("film_name", "equilibrium")
      .set("renamed_category", "action")
      .set("renamed_actor", "cathie")
      .build();
  }

  private GenericRecord getAvatarRecord1(org.apache.avro.Schema avroOutputSchema) {
    return new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "3")
      .set("film_name", "avatar")
      .set("renamed_actor", "samuel")
      .set("renamed_category", null)
      .build();
  }
}
