/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    List<JoinField> outputFieldInfos = new ArrayList<>();
    outputFieldInfos.add(new JoinField("id", "customer", "id"));
    outputFieldInfos.add(new JoinField("customer_id", "purchase", "customer_id"));
    Set<JoinKey> joinKeys = new HashSet<>();
    joinKeys.add(new JoinKey("customer", Collections.singletonList("id")));
    joinKeys.add(new JoinKey("purchase", Collections.singletonList("customer_id")));
    List<FieldOperation> fieldOperations = Joiner.createFieldOperations(outputFieldInfos, joinKeys);
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


    List<JoinField> outputFieldInfos = new ArrayList<>();
    outputFieldInfos.add(new JoinField("id", "customer", "id"));
    outputFieldInfos.add(new JoinField("name", "customer", "name"));
    outputFieldInfos.add(new JoinField("customer_id", "purchase", "customer_id"));
    outputFieldInfos.add(new JoinField("item", "purchase", "item"));
    Set<JoinKey> joinKeys = new HashSet<>();
    joinKeys.add(new JoinKey("customer", Collections.singletonList("id")));
    joinKeys.add(new JoinKey("purchase", Collections.singletonList("customer_id")));
    List<FieldOperation> fieldOperations = Joiner.createFieldOperations(outputFieldInfos, joinKeys);
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

    List<JoinField> outputFieldInfos = new ArrayList<>();
    outputFieldInfos.add(new JoinField("id_from_customer", "customer", "id"));
    outputFieldInfos.add(new JoinField("id_from_purchase", "purchase", "customer_id"));
    Set<JoinKey> joinKeys = new HashSet<>();
    joinKeys.add(new JoinKey("customer", Collections.singletonList("id")));
    joinKeys.add(new JoinKey("purchase", Collections.singletonList("customer_id")));

    List<FieldOperation> fieldOperations = Joiner.createFieldOperations(outputFieldInfos, joinKeys);

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
    List<JoinField> outputFieldInfos = new ArrayList<>();
    outputFieldInfos.add(new JoinField("id_from_customer", "customer", "id"));
    outputFieldInfos.add(new JoinField("name_from_customer", "customer", "name"));
    outputFieldInfos.add(new JoinField("customer_id", "purchase", "customer_id"));
    outputFieldInfos.add(new JoinField("item_from_purchase", "purchase", "item"));
    Set<JoinKey> joinKeys = new HashSet<>();
    joinKeys.add(new JoinKey("customer", Collections.singletonList("id")));
    joinKeys.add(new JoinKey("purchase", Collections.singletonList("customer_id")));
    List<FieldOperation> fieldOperations = Joiner.createFieldOperations(outputFieldInfos, joinKeys);
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

    List<JoinField> outputFieldInfos = new ArrayList<>();
    outputFieldInfos.add(new JoinField("id_from_customer", "customer", "id"));
    outputFieldInfos.add(new JoinField("name_from_customer", "customer", "name"));
    outputFieldInfos.add(new JoinField("customer_id", "purchase", "customer_id"));
    outputFieldInfos.add(new JoinField("address_id", "address", "address_id"));
    outputFieldInfos.add(new JoinField("address", "address", "address"));
    Set<JoinKey> joinKeys = new HashSet<>();
    joinKeys.add(new JoinKey("customer", Collections.singletonList("id")));
    joinKeys.add(new JoinKey("purchase", Collections.singletonList("customer_id")));
    joinKeys.add(new JoinKey("address", Collections.singletonList("address_id")));
    List<FieldOperation> fieldOperations = Joiner.createFieldOperations(outputFieldInfos, joinKeys);
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
