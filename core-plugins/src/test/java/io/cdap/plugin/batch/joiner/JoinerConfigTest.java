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
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure.Cause;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Test cases for {@link JoinerConfig}.
 */
public class JoinerConfigTest {
  private static final Schema filmSchema = Schema.recordOf(
    "film",
    Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)));

  private static final Schema filmActorSchema = Schema.recordOf(
    "filmActor",
    Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("actor_name", Schema.of(Schema.Type.STRING)));

  private static final Schema filmCategorySchema = Schema.recordOf(
    "filmCategory",
    Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("category_name", Schema.of(Schema.Type.STRING)));

  // output schema sorted by selected fields
  private static final Schema outputSchema = Schema.recordOf(
    "joined",
    Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("renamed_actor", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("renamed_category", Schema.of(Schema.Type.STRING)));

  private static final String selectedFields = "film.film_id, film.film_name, " +
    "filmActor.actor_name as renamed_actor, filmCategory.category_name as renamed_category";

  private static final String STAGE = "stage";
  private static final String MOCK_STAGE = "mockstage";

  @Test
  public void testJoinerConfig() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");

    Joiner joiner = new Joiner(config);
    Map<String, Schema> inputSchemas = ImmutableMap.of("film", filmSchema, "filmActor", filmActorSchema,
                                                       "filmCategory", filmCategorySchema);
    FailureCollector collector = new MockFailureCollector();
    joiner.init(inputSchemas, collector);
    Schema actualOutputSchema = joiner.getOutputSchema(inputSchemas, collector);
    Assert.assertEquals(outputSchema, actualOutputSchema);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJoinerConfigWithJoinKeys() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");

    Assert.assertEquals(ImmutableMap.of("film", ImmutableList.of("film_id", "film_name"),
                                        "filmActor", ImmutableList.of("film_id", "film_name"),
                                        "filmCategory", ImmutableList.of("film_id", "film_name")),
                        config.getPerStageJoinKeys());
  }

  @Test
  public void testJoinerConfigWithRequiredInputs() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");
    Assert.assertEquals(ImmutableSet.of("film", "filmActor", "filmCategory"), config.getInputs());
  }

  @Test
  public void testJoinerConfigWithSelectedFields() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");
    ImmutableTable.Builder<String, String, String> expected =  new ImmutableTable.Builder<>();
    expected.put("film", "film_id", "film_id");
    expected.put("film", "film_name", "film_name");
    expected.put("filmActor", "actor_name", "renamed_actor");
    expected.put("filmCategory", "category_name", "renamed_category");
    Assert.assertEquals(expected.build(), config.getPerStageSelectedFields());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJoinerConfigWithoutJoinKeys() {
    JoinerConfig config = new JoinerConfig("", selectedFields, "film,filmActor,filmCategory");
    config.getPerStageJoinKeys();
  }

  @Test
  public void testJoinerConfigWithoutRequiredInputs() {
    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_actor", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("renamed_category", Schema.nullableOf(Schema.of(Schema.Type.STRING))));


    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film");

    Joiner joiner = new Joiner(config);
    Map<String, Schema> inputSchemas = ImmutableMap.of("film", filmSchema, "filmActor", filmActorSchema,
                                                       "filmCategory", filmCategorySchema);
    FailureCollector collector = new MockFailureCollector();
    joiner.init(inputSchemas, collector);
    Schema actualOutputSchema = joiner.getOutputSchema(inputSchemas, collector);
    Assert.assertEquals(outputSchema, actualOutputSchema);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJoinerConfigWithoutSelectedFields() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name", "",
                                           "film,filmActor,filmCategory");

    config.getPerStageSelectedFields();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJoinerConfigWithWrongJoinKeys() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");
    config.getPerStageJoinKeys();
  }

  @Test
  public void testJoinerConfigWithoutFieldsToRename() {
    String selectedFields = "film.film_id, film.film_name, " +
      "filmActor.actor_name as renamed_actor, filmCategory.category_name as renamed_category";

    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");

    Table<String, String, String> actual = config.getPerStageSelectedFields();
    ImmutableTable.Builder<String, String, String> tableBuilder = new ImmutableTable.Builder<>();
    tableBuilder.put("film", "film_id", "film_id");
    tableBuilder.put("film", "film_name", "film_name");
    tableBuilder.put("filmActor", "actor_name", "renamed_actor");
    tableBuilder.put("filmCategory", "category_name", "renamed_category");
    Table<String, String, String> expected = tableBuilder.build();
    Assert.assertEquals(expected, actual);

  }

  @Test
  public void testJoinerConfigWithDuplicateOutputFields() {
    String selectedFields = "film.film_id, film.film_name, " +
      "filmActor.actor_name as name, filmCategory.category_name as name";

    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");
    Joiner joiner = new Joiner(config);
    Map<String, Schema> inputSchemas = ImmutableMap.of("film", filmSchema, "filmActor", filmActorSchema,
                                                       "filmCategory", filmCategorySchema);
    FailureCollector collector = new MockFailureCollector();
    try {
      joiner.init(inputSchemas, collector);
      joiner.getOutputSchema(inputSchemas, collector);
      Assert.fail();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(1, e.getFailures().get(0).getCauses().size());
      Cause expectedCause = new Cause();
      expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, JoinerConfig.SELECT_FIELDS);
      expectedCause.addAttribute(STAGE, MOCK_STAGE);
      Assert.assertEquals(expectedCause, e.getFailures().get(0).getCauses().get(0));
    }
  }

  @Test
  public void testJoinerConfigWithInvalidJoinKeys() {
    String selectedFields = "film.film_id, film.film_name, " +
      "filmActor.actor_name as renamed_actor, filmCategory.category_name as renamed_category";

    Schema filmCategorySchema = Schema.recordOf(
      "filmCategory",
      Schema.Field.of("film_id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("category_name", Schema.of(Schema.Type.STRING)));

    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");

    Joiner joiner = new Joiner(config);
    FailureCollector collector = new MockFailureCollector();
    joiner.validateJoinKeySchemas(
        ImmutableMap.of("film", filmSchema, "filmActor", filmActorSchema,
            "filmCategory", filmCategorySchema), config.getPerStageJoinKeys(), collector);
    Assert.assertEquals(2, collector.getValidationFailures().size());
    // Assert first failure
    Assert.assertEquals(1, collector.getValidationFailures().get(0).getCauses().size());
    Cause expectedCause = new Cause();
    expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, JoinerConfig.JOIN_KEYS);
    Assert.assertEquals(expectedCause, collector.getValidationFailures().get(0).getCauses().get(0));
    // Assert second failure
    Assert.assertEquals(1, collector.getValidationFailures().get(1).getCauses().size());
    Assert.assertEquals(expectedCause, collector.getValidationFailures().get(1).getCauses().get(0));
  }

  @Test
  public void testJoinerOutputSchema() {
    Map<String, Schema> inputSchemas = ImmutableMap.of("film", filmSchema, "filmActor", filmActorSchema,
                                                    "filmCategory", filmCategorySchema);
    String joinKeys = "film.film_id=filmActor.film_id=filmCategory.film_id";
    String selectedFields = "film.film_id, film.film_name, filmActor.actor_name as renamed_actor, " +
      "filmCategory.category_name as renamed_category";
    String requiredInputs = "film,filmActor,filmCategory";
    JoinerConfig joinerConfig = new JoinerConfig(joinKeys, selectedFields, requiredInputs);

    Joiner joiner = new Joiner(joinerConfig);
    FailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(outputSchema, joiner.getOutputSchema(inputSchemas, collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testOutputSchemaForInvalidKeys() {
    // film_id is Long but it should be String, OutputSchema call should throw an exception
    Schema filmCategorySchema = Schema.recordOf(
      "filmCategory",
      Schema.Field.of("film_id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("category_name", Schema.of(Schema.Type.STRING)));

    Map<String, Schema> inputSchemas = ImmutableMap.of("film", filmSchema, "filmActor", filmActorSchema,
                                                       "filmCategory", filmCategorySchema);

    String joinKeys = "film.film_id=filmActor.film_id=filmCategory.film_id";
    String selectedFields = "film.film_id, film.film_name, filmActor.actor_name as renamed_actor, " +
      "filmCategory.category_name as renamed_category";
    String requiredInputs = "film,filmActor,filmCategory";
    JoinerConfig config = new JoinerConfig(joinKeys, selectedFields, requiredInputs);

    Joiner joiner = new Joiner(config);
    FailureCollector collector = new MockFailureCollector();
    try {
      joiner.getOutputSchema(inputSchemas, collector);
      Assert.fail();
    } catch (ValidationException e) {
      Assert.assertEquals(2, e.getFailures().size());
      // Assert first failure
      Assert.assertEquals(1, e.getFailures().get(0).getCauses().size());
      Cause expectedCause = new Cause();
      expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, JoinerConfig.JOIN_KEYS);
      expectedCause.addAttribute(STAGE, MOCK_STAGE);
      Assert.assertEquals(expectedCause, e.getFailures().get(0).getCauses().get(0));
      // Assert second failure
      Assert.assertEquals(1, e.getFailures().get(1).getCauses().size());
      Assert.assertEquals(expectedCause, e.getFailures().get(1).getCauses().get(0));
    }
  }

  @Test
  public void testJoinerWithNullableSchema() {
    Schema filmCategorySchema = Schema.recordOf(
      "filmCategory",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("category_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Map<String, Schema> inputSchemas = ImmutableMap.of("film", filmSchema, "filmActor", filmActorSchema,
                                                    "filmCategory", filmCategorySchema);
    String joinKeys = "film.film_id=filmActor.film_id=filmCategory.film_id";
    String selectedFields = "film.film_id, film.film_name, filmActor.actor_name as renamed_actor, " +
      "filmCategory.category_name as renamed_category";
    String requiredInputs = "film,filmActor";
    JoinerConfig conf = new JoinerConfig(joinKeys, selectedFields, requiredInputs);

    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_actor", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_category", filmCategorySchema.getField("category_name").getSchema()));

    Joiner joiner = new Joiner(conf);
    FailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(outputSchema, joiner.getOutputSchema(inputSchemas, collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

}
