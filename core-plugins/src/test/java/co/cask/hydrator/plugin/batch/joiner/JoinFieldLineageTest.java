/*
 * Copyright © 2018 Cask Data, Inc.
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
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldTransformOperation;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class JoinFieldLineageTest {

  @Test
  public void testSimpleJoinOperations() {
    //  customer -> (id)------------
    //                              |
    //                            JOIN  ------->(id, customer_id)
    //                              |
    //  purchase -> (customer_id)---

    List<Joiner.OutputFieldInfo> outputFieldInfos = new ArrayList<>();
    outputFieldInfos.add(new Joiner.OutputFieldInfo("id", "customer", "id",
                                                    Schema.Field.of("id", Schema.of(Schema.Type.INT))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("customer_id", "purchase", "customer_id",
                                                    Schema.Field.of("customer_id", Schema.of(Schema.Type.INT))));
    Map<String, List<String>> perStageJoinKeys = new LinkedHashMap<>();
    perStageJoinKeys.put("customer", Collections.singletonList("id"));
    perStageJoinKeys.put("purchase", Collections.singletonList("customer_id"));
    List<FieldOperation> fieldOperations = Joiner.createFieldOperations(outputFieldInfos, perStageJoinKeys);
    FieldOperation operation = new FieldTransformOperation("Join", Joiner.JOIN_OPERATION_DESCRIPTION,
                                                           Arrays.asList("customer.id", "purchase.customer_id"),
                                                           Arrays.asList("id", "customer_id"));

    Assert.assertEquals(Collections.singletonList(operation), fieldOperations);
  }

  @Test
  public void testSimpleJoinWithAdditionalFields() {
    //  customer -> (id, name)----------
    //                                  |
    //                                JOIN  ------->(id, customer_id, name, item)
    //                                  |
    //  purchase ->(customer_id, item)---


    List<Joiner.OutputFieldInfo> outputFieldInfos = new ArrayList<>();
    outputFieldInfos.add(new Joiner.OutputFieldInfo("id", "customer", "id",
                                                    Schema.Field.of("id", Schema.of(Schema.Type.INT))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("name", "customer", "name",
                                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("customer_id", "purchase", "customer_id",
                                                    Schema.Field.of("customer_id", Schema.of(Schema.Type.INT))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("item", "purchase", "item",
                                                    Schema.Field.of("item", Schema.of(Schema.Type.STRING))));
    Map<String, List<String>> perStageJoinKeys = new LinkedHashMap<>();
    perStageJoinKeys.put("customer", Collections.singletonList("id"));
    perStageJoinKeys.put("purchase", Collections.singletonList("customer_id"));
    List<FieldOperation> fieldOperations = Joiner.createFieldOperations(outputFieldInfos, perStageJoinKeys);
    List<FieldOperation> expected = new ArrayList<>();

    expected.add(new FieldTransformOperation("Join", Joiner.JOIN_OPERATION_DESCRIPTION,
                                             Arrays.asList("customer.id", "purchase.customer_id"),
                                             Arrays.asList("id", "customer_id")));
    expected.add(new FieldTransformOperation("Identity customer.name", Joiner.IDENTITY_OPERATION_DESCRIPTION,
                                             Collections.singletonList("customer.name"),
                                             Collections.singletonList("name")));
    expected.add(new FieldTransformOperation("Identity purchase.item", Joiner.IDENTITY_OPERATION_DESCRIPTION,
                                             Collections.singletonList("purchase.item"),
                                             Collections.singletonList("item")));
    Assert.assertEquals(expected, fieldOperations);
  }

  @Test
  public void testSimpleJoinWithRenameJoinKeys() {
    //  customer -> (id, name)----------
    //                                  |
    //                                JOIN  ------->(id_from_customer, id_from_purchase, name, item)
    //                                  |
    //  purchase ->(customer_id, item)---

    List<Joiner.OutputFieldInfo> outputFieldInfos = new ArrayList<>();
    outputFieldInfos.add(new Joiner.OutputFieldInfo("id_from_customer", "customer", "id",
                                                    Schema.Field.of("id", Schema.of(Schema.Type.INT))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("id_from_purchase", "purchase", "customer_id",
                                                    Schema.Field.of("customer_id", Schema.of(Schema.Type.INT))));
    Map<String, List<String>> perStageJoinKeys = new LinkedHashMap<>();
    perStageJoinKeys.put("customer", Collections.singletonList("id"));
    perStageJoinKeys.put("purchase", Collections.singletonList("customer_id"));

    List<FieldOperation> fieldOperations = Joiner.createFieldOperations(outputFieldInfos, perStageJoinKeys);

    List<FieldOperation> expected = new ArrayList<>();
    expected.add(new FieldTransformOperation("Join", Joiner.JOIN_OPERATION_DESCRIPTION,
                                             Arrays.asList("customer.id", "purchase.customer_id"),
                                             Arrays.asList("id", "customer_id")));
    expected.add(new FieldTransformOperation("Rename id", Joiner.RENAME_OPERATION_DESCRIPTION,
                                             Collections.singletonList("id"),
                                             Collections.singletonList("id_from_customer")));
    expected.add(new FieldTransformOperation("Rename customer_id", Joiner.RENAME_OPERATION_DESCRIPTION,
                                             Collections.singletonList("customer_id"),
                                             Collections.singletonList("id_from_purchase")));
    Assert.assertEquals(expected, fieldOperations);
  }

  @Test
  public void testSimpleJoinWithRenameOnAdditionalFields() {
    //  customer -> (id, name)----------
    //                                  |
    //                                JOIN  --->(id_from_customer, customer_id, name_from_customer, item_from_purchase)
    //                                  |
    //  purchase ->(customer_id, item)---
    List<Joiner.OutputFieldInfo> outputFieldInfos = new ArrayList<>();
    outputFieldInfos.add(new Joiner.OutputFieldInfo("id_from_customer", "customer", "id",
                                                    Schema.Field.of("id", Schema.of(Schema.Type.INT))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("name_from_customer", "customer", "name",
                                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("customer_id", "purchase", "customer_id",
                                                    Schema.Field.of("customer_id", Schema.of(Schema.Type.INT))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("item_from_purchase", "purchase", "item",
                                                    Schema.Field.of("item", Schema.of(Schema.Type.STRING))));
    Map<String, List<String>> perStageJoinKeys = new LinkedHashMap<>();
    perStageJoinKeys.put("customer", Collections.singletonList("id"));
    perStageJoinKeys.put("purchase", Collections.singletonList("customer_id"));
    List<FieldOperation> fieldOperations = Joiner.createFieldOperations(outputFieldInfos, perStageJoinKeys);
    List<FieldOperation> expected = new ArrayList<>();

    expected.add(new FieldTransformOperation("Join", Joiner.JOIN_OPERATION_DESCRIPTION,
                                             Arrays.asList("customer.id", "purchase.customer_id"),
                                             Arrays.asList("id", "customer_id")));
    expected.add(new FieldTransformOperation("Rename id", Joiner.RENAME_OPERATION_DESCRIPTION,
                                             Collections.singletonList("id"),
                                             Collections.singletonList("id_from_customer")));
    expected.add(new FieldTransformOperation("Rename customer.name", Joiner.RENAME_OPERATION_DESCRIPTION,
                                             Collections.singletonList("customer.name"),
                                             Collections.singletonList("name_from_customer")));
    expected.add(new FieldTransformOperation("Rename purchase.item", Joiner.RENAME_OPERATION_DESCRIPTION,
                                             Collections.singletonList("purchase.item"),
                                             Collections.singletonList("item_from_purchase")));
    Assert.assertEquals(expected, fieldOperations);
  }

  @Test
  public void testJoinWith3Inputs() {
    // customer -> (id, name)---------- |
    //                                  |
    // purchase ->(customer_id, item)------> JOIN --->(id_from_customer, customer_id, address_id,
    //                                  |                   name_from_customer, address)
    //                                  |
    // address ->(address_id, address)--|

    List<Joiner.OutputFieldInfo> outputFieldInfos = new ArrayList<>();
    outputFieldInfos.add(new Joiner.OutputFieldInfo("id_from_customer", "customer", "id",
                                                    Schema.Field.of("id", Schema.of(Schema.Type.INT))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("name_from_customer", "customer", "name",
                                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("customer_id", "purchase", "customer_id",
                                                    Schema.Field.of("customer_id", Schema.of(Schema.Type.INT))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("address_id", "address", "address_id",
                                                    Schema.Field.of("address_id", Schema.of(Schema.Type.INT))));
    outputFieldInfos.add(new Joiner.OutputFieldInfo("address", "address", "address",
                                                    Schema.Field.of("address", Schema.of(Schema.Type.STRING))));
    Map<String, List<String>> perStageJoinKeys = new LinkedHashMap<>();
    perStageJoinKeys.put("customer", Collections.singletonList("id"));
    perStageJoinKeys.put("purchase", Collections.singletonList("customer_id"));
    perStageJoinKeys.put("address", Collections.singletonList("address_id"));
    List<FieldOperation> fieldOperations = Joiner.createFieldOperations(outputFieldInfos, perStageJoinKeys);
    List<FieldOperation> expected = new ArrayList<>();

    expected.add(new FieldTransformOperation("Join", Joiner.JOIN_OPERATION_DESCRIPTION,
                                             Arrays.asList("customer.id", "purchase.customer_id", "address.address_id"),
                                             Arrays.asList("id", "customer_id", "address_id")));

    expected.add(new FieldTransformOperation("Rename id", Joiner.RENAME_OPERATION_DESCRIPTION,
                                             Collections.singletonList("id"),
                                             Collections.singletonList("id_from_customer")));

    expected.add(new FieldTransformOperation("Rename customer.name", Joiner.RENAME_OPERATION_DESCRIPTION,
                                             Collections.singletonList("customer.name"),
                                             Collections.singletonList("name_from_customer")));

    expected.add(new FieldTransformOperation("Identity address.address", Joiner.IDENTITY_OPERATION_DESCRIPTION,
                                             Collections.singletonList("address.address"),
                                             Collections.singletonList("address")));

    Assert.assertEquals(expected, fieldOperations);
  }
}
