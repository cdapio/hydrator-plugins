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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.api.validation.ValidationFailure.Cause;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test cases for {@link JoinerConfig}.
 */
public class JoinerConfigTest {
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

  // output schema sorted by selected fields
  private static final Schema OUTPUT_SCHEMA = Schema.recordOf(
    "join.output",
    Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("renamed_actor", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("renamed_category", Schema.of(Schema.Type.STRING)));

  private static final String SELECTED_FIELDS = "film.film_id, film.film_name, " +
    "filmActor.actor_name as renamed_actor, filmCategory.category_name as renamed_category";

  private static final Map<String, JoinStage> INPUT_STAGES = ImmutableMap.of(
    "film", JoinStage.builder("film", FILM_SCHEMA).build(),
    "filmActor", JoinStage.builder("filmActor", FILM_ACTOR_SCHEMA).build(),
    "filmCategory", JoinStage.builder("filmCategory", FILM_CATEGORY_SCHEMA).build());

  private static final String STAGE = "stage";
  private static final String MOCK_STAGE = "mockstage";

  @Test
  public void testJoinerConfig() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           SELECTED_FIELDS, "film,filmActor,filmCategory");

    Joiner joiner = new Joiner(config);
    FailureCollector collector = new MockFailureCollector();
    AutoJoinerContext autoJoinerContext = new MockAutoJoinerContext(INPUT_STAGES, collector);

    JoinDefinition joinDefinition = joiner.define(autoJoinerContext);
    Assert.assertEquals(OUTPUT_SCHEMA, joinDefinition.getOutputSchema());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJoinerConfigWithJoinKeys() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           SELECTED_FIELDS, "film,filmActor,filmCategory");

    Set<JoinKey> expected = new HashSet<>(Arrays.asList(
      new JoinKey("film", Arrays.asList("film_id", "film_name")),
      new JoinKey("filmActor", Arrays.asList("film_id", "film_name")),
      new JoinKey("filmCategory", Arrays.asList("film_id", "film_name"))));
    Assert.assertEquals(expected, config.getJoinKeys(new MockFailureCollector()));
  }

  @Test
  public void testJoinerConfigWithRequiredInputs() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           SELECTED_FIELDS, "film,filmActor,filmCategory");
    Assert.assertEquals(ImmutableSet.of("film", "filmActor", "filmCategory"), config.getRequiredInputs());
  }

  @Test
  public void testJoinerConfigWithSelectedFields() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           SELECTED_FIELDS, "film,filmActor,filmCategory");
    List<JoinField> expected = Arrays.asList(
      new JoinField("film", "film_id", "film_id"),
      new JoinField("film", "film_name", "film_name"),
      new JoinField("filmActor", "actor_name", "renamed_actor"),
      new JoinField("filmCategory", "category_name", "renamed_category"));
    Assert.assertEquals(expected, config.getSelectedFields(new MockFailureCollector()));
  }

  @Test
  public void testJoinerConfigWithoutJoinKeys() {
    JoinerConfig config = new JoinerConfig("", SELECTED_FIELDS, "film,filmActor,filmCategory");
    MockFailureCollector failureCollector = new MockFailureCollector();
    try {
      config.getJoinKeys(failureCollector);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure failure = e.getFailures().get(0);
      Assert.assertEquals(1, failure.getCauses().size());
    }
  }

  @Test
  public void testJoinerConfigWithoutRequiredInputs() {
    Schema outputSchema = Schema.recordOf(
      "join.output",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_actor", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("renamed_category", Schema.nullableOf(Schema.of(Schema.Type.STRING))));


    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           SELECTED_FIELDS, "film");

    Joiner joiner = new Joiner(config);
    FailureCollector collector = new MockFailureCollector();
    AutoJoinerContext autoJoinerContext = new MockAutoJoinerContext(INPUT_STAGES, collector);
    JoinDefinition joinDefinition = joiner.define(autoJoinerContext);
    Assert.assertEquals(outputSchema, joinDefinition.getOutputSchema());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJoinerConfigWithoutSelectedFields() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name", "",
                                           "film,filmActor,filmCategory");
    FailureCollector failureCollector = new MockFailureCollector();
    try {
      config.getSelectedFields(failureCollector);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure failure = e.getFailures().get(0);
      Assert.assertEquals(1, failure.getCauses().size());
      Cause cause = failure.getCauses().get(0);
      Assert.assertEquals(JoinerConfig.SELECTED_FIELDS, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testJoinerConfigWithWrongJoinKeys() {
    JoinerConfig config = new JoinerConfig("film.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           SELECTED_FIELDS, "film,filmActor,filmCategory");
    FailureCollector failureCollector = new MockFailureCollector();
    try {
      config.getJoinKeys(failureCollector);
      Assert.fail();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure failure = e.getFailures().get(0);
      Assert.assertEquals(1, failure.getCauses().size());
      ValidationFailure.Cause cause = failure.getCauses().get(0);
      Assert.assertEquals(JoinerConfig.JOIN_KEYS, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testJoinerConfigWithoutFieldsToRename() {
    String selectedFields = "film.film_id, film.film_name, " +
      "filmActor.actor_name as renamed_actor, filmCategory.category_name as renamed_category";

    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");

    FailureCollector failureCollector = new MockFailureCollector();
    List<JoinField> actual = config.getSelectedFields(failureCollector);
    List<JoinField> expected = Arrays.asList(
      new JoinField("film", "film_id", "film_id"),
      new JoinField("film", "film_name", "film_name"),
      new JoinField("filmActor", "actor_name", "renamed_actor"),
      new JoinField("filmCategory", "category_name", "renamed_category"));
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
    FailureCollector collector = new MockFailureCollector();
    AutoJoinerContext autoJoinerContext = new MockAutoJoinerContext(INPUT_STAGES, collector);
    try {
      joiner.define(autoJoinerContext);
      Assert.fail();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(1, e.getFailures().get(0).getCauses().size());
      Cause expectedCause = new Cause();
      expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, JoinerConfig.SELECTED_FIELDS);
      expectedCause.addAttribute(STAGE, MOCK_STAGE);
      expectedCause.addAttribute(CauseAttributes.CONFIG_ELEMENT, "filmCategory.category_name as name");
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
    Map<String, JoinStage> inputStages = new HashMap<>();
    inputStages.put("film", JoinStage.builder("film", FILM_SCHEMA).build());
    inputStages.put("filmActor", JoinStage.builder("filmActor", FILM_ACTOR_SCHEMA).build());
    inputStages.put("fileCategory", JoinStage.builder("filmCategory", filmCategorySchema).build());
    AutoJoinerContext autoJoinerContext = new MockAutoJoinerContext(inputStages, collector);
    try {
      joiner.define(autoJoinerContext);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(1, e.getFailures().get(0).getCauses().size());
      Cause expectedCause = new Cause();
      expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, JoinerConfig.JOIN_KEYS);
      expectedCause.addAttribute("stage", "mockstage");
      Assert.assertEquals(JoinerConfig.JOIN_KEYS,
                          e.getFailures().get(0).getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testJoinerOutputSchema() {
    String joinKeys = "film.film_id=filmActor.film_id=filmCategory.film_id";
    String selectedFields = "film.film_id, film.film_name, filmActor.actor_name as renamed_actor, " +
      "filmCategory.category_name as renamed_category";
    String requiredInputs = "film,filmActor,filmCategory";
    JoinerConfig joinerConfig = new JoinerConfig(joinKeys, selectedFields, requiredInputs);

    Joiner joiner = new Joiner(joinerConfig);
    FailureCollector collector = new MockFailureCollector();
    AutoJoinerContext autoJoinerContext = new MockAutoJoinerContext(INPUT_STAGES, collector);
    JoinDefinition joinDefinition = joiner.define(autoJoinerContext);
    Assert.assertEquals(OUTPUT_SCHEMA, joinDefinition.getOutputSchema());
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

    String joinKeys = "film.film_id=filmActor.film_id=filmCategory.film_id";
    String selectedFields = "film.film_id, film.film_name, filmActor.actor_name as renamed_actor, " +
      "filmCategory.category_name as renamed_category";
    String requiredInputs = "film,filmActor,filmCategory";
    JoinerConfig config = new JoinerConfig(joinKeys, selectedFields, requiredInputs);

    Joiner joiner = new Joiner(config);
    FailureCollector collector = new MockFailureCollector();
    Map<String, JoinStage> inputStages = new HashMap<>();
    inputStages.put("film", JoinStage.builder("film", FILM_SCHEMA).build());
    inputStages.put("filmActor", JoinStage.builder("filmActor", FILM_ACTOR_SCHEMA).build());
    inputStages.put("filmCategory", JoinStage.builder("filmCategory", filmCategorySchema).build());
    AutoJoinerContext autoJoinerContext = new MockAutoJoinerContext(inputStages, collector);
    try {
      joiner.define(autoJoinerContext);
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

    Map<String, Schema> inputSchemas = ImmutableMap.of("film", FILM_SCHEMA, "filmActor", FILM_ACTOR_SCHEMA,
                                                       "filmCategory", filmCategorySchema);
    List<String> inputStages = Arrays.asList("film", "filmActor", "filmCategory");

    String joinKeys = "film.film_id=filmActor.film_id=filmCategory.film_id";
    String selectedFields = "film.film_id, film.film_name, filmActor.actor_name as renamed_actor, " +
      "filmCategory.category_name as renamed_category";
    String requiredInputs = "film,filmActor";
    JoinerConfig conf = new JoinerConfig(joinKeys, selectedFields, requiredInputs);

    Schema outputSchema = Schema.recordOf(
      "join.output",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_actor", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_category", filmCategorySchema.getField("category_name").getSchema()));

    Joiner joiner = new Joiner(conf);
    FailureCollector collector = new MockFailureCollector();
    AutoJoinerContext autoJoinerContext = new MockAutoJoinerContext(INPUT_STAGES, collector);
    JoinDefinition joinDefinition = joiner.define(autoJoinerContext);
    Assert.assertEquals(outputSchema, joinDefinition.getOutputSchema());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

}
