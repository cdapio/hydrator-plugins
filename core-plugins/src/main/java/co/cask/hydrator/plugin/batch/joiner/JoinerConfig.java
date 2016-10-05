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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.KeyValueListParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Config for join plugin.
 */
public class JoinerConfig extends PluginConfig {
  private static final String NUM_PARTITIONS_DESC = "Number of partitions to use when joining. " +
    "If not specified, the execution framework will decide how many to use.";
  private static final String JOIN_KEY_DESC = "List of join keys to perform join operation. The list is " +
    "separated by '&'. Join key from each input stage will be prefixed with '<stageName>.' And the " +
    "relation among join keys from different inputs is represented by '='. For example: " +
    "customers.customer_id=items.c_id&customers.customer_name=items.c_name means the join key is a composite key" +
    " of customer id and customer name from customers and items input stages and join will be performed on equality " +
    "of the join keys.";
  private static final String SELECTED_FIELDS = "Comma-separated list of fields to be selected and renamed " +
    "in join output from each input stage. Each selected input field name needs to be present in the output must be " +
    "prefixed with '<input_stage_name>'. The syntax for specifying alias for each selected field is similar to sql. " +
    "For example: customers.id as customer_id, customer.name as customer_name, item.id as item_id, " +
    "<stageName>.inputFieldName as alias. The output will have same order of fields as selected in selectedFields." +
    "There must not be any duplicate fields in output.";
  private static final String REQUIRED_INPUTS_DESC = "Comma-separated list of stages." +
    " Required input stages decide the type of the join. If all the input stages are present in required inputs, " +
    "inner join will be performed. Otherwise, outer join will be performed considering non-required inputs as " +
    "optional.";


  @Nullable
  @Description(NUM_PARTITIONS_DESC)
  protected Integer numPartitions;

  @Description(JOIN_KEY_DESC)
  protected String joinKeys;

  @Description(SELECTED_FIELDS)
  protected String selectedFields;

  @Nullable
  @Description(REQUIRED_INPUTS_DESC)
  protected String requiredInputs;

  public JoinerConfig() {
    this.joinKeys = "";
    this.selectedFields = "";
    this.requiredInputs = null;
  }

  @VisibleForTesting
  JoinerConfig(String joinKeys, String selectedFields, String requiredInputs) {
    this.joinKeys = joinKeys;
    this.selectedFields = selectedFields;
    this.requiredInputs = requiredInputs;
  }

  @Nullable
  public Integer getNumPartitions() {
    return numPartitions;
  }

  public String getSelectedFields() {
    return selectedFields;
  }

  public String getJoinKeys() {
    return joinKeys;
  }

  @Nullable
  public String getRequiredInputs() {
    return requiredInputs;
  }


  Map<String, List<String>> getPerStageJoinKeys() {
    Map<String, List<String>> stageToKey = new HashMap<>();

    if (Strings.isNullOrEmpty(joinKeys)) {
      throw new IllegalArgumentException("Join keys can not be empty");
    }

    Iterable<String> multipleJoinKeys = Splitter.on('&').trimResults().omitEmptyStrings().split(joinKeys);

    if (Iterables.isEmpty(multipleJoinKeys)) {
      throw new IllegalArgumentException("Join keys can not be empty.");
    }

    int numJoinKeys = 0;
    for (String singleJoinKey : multipleJoinKeys) {
      KeyValueListParser kvParser = new KeyValueListParser("\\s*=\\s*", "\\.");
      Iterable<KeyValue<String, String>> keyValues = kvParser.parse(singleJoinKey);
      if (numJoinKeys == 0) {
        numJoinKeys = Iterables.size(keyValues);
      } else if (numJoinKeys != Iterables.size(keyValues)) {
        throw new IllegalArgumentException("There should be one join key from each of the stages. Please add join " +
                                             "keys for each stage.");
      }
      for (KeyValue<String, String> keyValue : keyValues) {
        String stageName = keyValue.getKey();
        String joinKey = keyValue.getValue();
        if (!stageToKey.containsKey(stageName)) {
          stageToKey.put(stageName, new ArrayList<String>());
        }
        stageToKey.get(stageName).add(joinKey);
      }
    }
    return stageToKey;
  }

  Table<String, String, String> getPerStageSelectedFields() {
    // table to store <stageName, oldFieldName, alias>
    ImmutableTable.Builder<String, String, String> tableBuilder = new ImmutableTable.Builder<>();

    if (Strings.isNullOrEmpty(selectedFields)) {
      throw new IllegalArgumentException("selectedFields can not be empty. Please provide at least 1 selectedFields");
    }

    for (String selectedField : Splitter.on(',').trimResults().omitEmptyStrings().split(selectedFields)) {
      Iterable<String> stageOldNameAliasPair = Splitter.on(" as ").trimResults().omitEmptyStrings()
        .split(selectedField);
      Iterable<String> stageOldNamePair = Splitter.on('.').trimResults().omitEmptyStrings().
        split(Iterables.get(stageOldNameAliasPair, 0));

      if (Iterables.size(stageOldNamePair) != 2) {
        throw new IllegalArgumentException(String.format("Invalid syntax. Selected Fields must be of syntax " +
                                                           "<stageName>.<oldFieldName> as <alias>, but found %s",
                                                         selectedField));
      }

      String stageName = Iterables.get(stageOldNamePair, 0);
      String oldFieldName = Iterables.get(stageOldNamePair, 1);

      // if alias is not present in selected fields, use original field name as alias
      String alias = isAliasPresent(stageOldNameAliasPair) ? oldFieldName : Iterables.get(stageOldNameAliasPair, 1);
      tableBuilder.put(stageName, oldFieldName, alias);
    }
    return tableBuilder.build();
  }

  private boolean isAliasPresent(Iterable<String> stageOldNameAliasPair) {
    return Iterables.size(stageOldNameAliasPair) == 1;
  }

  Set<String> getInputs() {
    if (!Strings.isNullOrEmpty(requiredInputs)) {
      return ImmutableSet.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().split(requiredInputs));
    }
    return ImmutableSet.of();
  }
}
