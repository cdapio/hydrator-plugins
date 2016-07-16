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

package co.cask.hydrator.plugin.batch.joiner;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.transform.MockPipelineConfigurer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import org.junit.Assert;
import org.junit.Test;

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

  private static final String selectedFields = "film.film_id as film_id, film.film_name as film_name, " +
    "filmActor.actor_name as renamed_actor, filmCategory.category_name as renamed_category";

  @Test
  public void testJoinerConfig() {
    // output schema sorted by selected filds
    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_actor", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_category", Schema.of(Schema.Type.STRING)));

    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");

    MockPipelineConfigurer pipelineConfigurer = new MockPipelineConfigurer(
      ImmutableMap.of("film", filmSchema, "filmActor", filmActorSchema,
                      "filmCategory", filmCategorySchema));
    Joiner joiner = new Joiner(config);
    joiner.configurePipeline(pipelineConfigurer);
    Assert.assertEquals(outputSchema, pipelineConfigurer.getOutputSchema());
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

    MockPipelineConfigurer pipelineConfigurer = new MockPipelineConfigurer(
      ImmutableMap.of("film", filmSchema, "filmActor", filmActorSchema,
                      "filmCategory", filmCategorySchema));
    Joiner joiner = new Joiner(config);
    joiner.configurePipeline(pipelineConfigurer);
    Assert.assertEquals(outputSchema, pipelineConfigurer.getOutputSchema());
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
    String selectedFields = "film.film_id film_id, film.film_name as film_name, " +
      "filmActor.actor_name as renamed_actor, filmCategory.category_name as renamed_category";

    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");
    config.getPerStageSelectedFields();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJoinerConfigWithDuplicateOutputFields() {
    String selectedFields = "film.film_id as id, film.film_name as name, " +
      "filmActor.actor_name as name, filmCategory.category_name as name";

    JoinerConfig config = new JoinerConfig("film.film_id=filmActor.film_id=filmCategory.film_id&" +
                                             "film.film_name=filmActor.film_name=filmCategory.film_name",
                                           selectedFields, "film,filmActor,filmCategory");
    MockPipelineConfigurer pipelineConfigurer = new MockPipelineConfigurer(
      ImmutableMap.of("film", filmSchema, "filmActor", filmActorSchema,
                      "filmCategory", filmCategorySchema));
    Joiner joiner = new Joiner(config);
    joiner.configurePipeline(pipelineConfigurer);
  }
}
