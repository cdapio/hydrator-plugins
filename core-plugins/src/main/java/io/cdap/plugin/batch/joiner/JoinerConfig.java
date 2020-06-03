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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.plugin.common.KeyValueListParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Config for join plugin.
 */
public class JoinerConfig extends PluginConfig {
  public static final String SELECTED_FIELDS = "selectedFields";
  public static final String REQUIRED_INPUTS = "requiredInputs";
  public static final String JOIN_KEYS = "joinKeys";
  public static final String OUTPUT_SCHEMA = "schema";
  private static final String NUM_PARTITIONS_DESC = "Number of partitions to use when joining. " +
    "If not specified, the execution framework will decide how many to use.";
  private static final String JOIN_KEY_DESC = "List of join keys to perform join operation. The list is " +
    "separated by '&'. Join key from each input stage will be prefixed with '<stageName>.' And the " +
    "relation among join keys from different inputs is represented by '='. For example: " +
    "customers.customer_id=items.c_id&customers.customer_name=items.c_name means the join key is a composite key" +
    " of customer id and customer name from customers and items input stages and join will be performed on equality " +
    "of the join keys.";
  private static final String SELECTED_FIELDS_DESC = "Comma-separated list of fields to be selected and renamed " +
    "in join output from each input stage. Each selected input field name needs to be present in the output must be " +
    "prefixed with '<input_stage_name>'. The syntax for specifying alias for each selected field is similar to sql. " +
    "For example: customers.id as customer_id, customer.name as customer_name, item.id as item_id, " +
    "<stageName>.inputFieldName as alias. The output will have same order of fields as selected in selectedFields." +
    "There must not be any duplicate fields in output.";
  private static final String REQUIRED_INPUTS_DESC = "Comma-separated list of stages." +
    " Required input stages decide the type of the join. If all the input stages are present in required inputs, " +
    "inner join will be performed. Otherwise, outer join will be performed considering non-required inputs as " +
    "optional.";

  @Macro
  @Nullable
  @Description(NUM_PARTITIONS_DESC)
  protected Integer numPartitions;

  @Description(JOIN_KEY_DESC)
  @Macro
  protected String joinKeys;

  @Description(SELECTED_FIELDS_DESC)
  @Macro
  protected String selectedFields;

  @Nullable
  @Description(REQUIRED_INPUTS_DESC)
  @Macro
  protected String requiredInputs;

  @Nullable
  @Macro
  @Description(OUTPUT_SCHEMA)
  private String schema;

  @Nullable
  @Macro
  @Description("Set of input stages to try to load completely in memory to perform an in-memory join. " +
    "This is just a hint to the underlying execution engine to try and perform an in-memory join. " +
    "Whether it is actually loaded into memory is up to the engine. This property is ignored when MapReduce is used.")
  private String inMemoryInputs;

  @Nullable
  @Macro
  @Description("Whether null values in the join key should be joined on. For example, if the join is on A.id = B.id " +
    "and this value is false, records with a null id from input stages A and B will not get joined together.")
  private Boolean joinNullKeys;

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

  @Nullable
  public Schema getOutputSchema(FailureCollector collector) {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null).withConfigProperty(OUTPUT_SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  boolean requiredPropertiesContainMacros() {
    return containsMacro(SELECTED_FIELDS) || containsMacro(REQUIRED_INPUTS) || containsMacro(JOIN_KEYS) ||
      containsMacro(OUTPUT_SCHEMA);
  }

  Set<JoinKey> getJoinKeys(FailureCollector failureCollector) {
    // Use a LinkedHashMap to maintain the ordering as the input config.
    // This helps making error report deterministic.
    Map<String, List<String>> stageToKey = new LinkedHashMap<>();

    if (Strings.isNullOrEmpty(joinKeys)) {
      failureCollector.addFailure("Join keys cannot be empty", null).withConfigProperty(JOIN_KEYS);
      throw failureCollector.getOrThrowException();
    }

    Iterable<String> multipleJoinKeys = Splitter.on('&').trimResults().omitEmptyStrings().split(joinKeys);

    if (Iterables.isEmpty(multipleJoinKeys)) {
      failureCollector.addFailure("Join keys cannot be empty", null).withConfigProperty(JOIN_KEYS);
      throw failureCollector.getOrThrowException();
    }

    int numJoinKeys = 0;
    for (String singleJoinKey : multipleJoinKeys) {
      KeyValueListParser kvParser = new KeyValueListParser("\\s*=\\s*", "\\.");
      Iterable<KeyValue<String, String>> keyValues = kvParser.parse(singleJoinKey);
      if (numJoinKeys == 0) {
        numJoinKeys = Iterables.size(keyValues);
      } else if (numJoinKeys != Iterables.size(keyValues)) {
        failureCollector.addFailure("There should be one join key from each of the stages",
                                    "Please add join keys for each stage.")
          .withConfigProperty(JOIN_KEYS);
        throw failureCollector.getOrThrowException();
      }
      for (KeyValue<String, String> keyValue : keyValues) {
        String stageName = keyValue.getKey();
        String joinKey = keyValue.getValue();
        if (!stageToKey.containsKey(stageName)) {
          stageToKey.put(stageName, new ArrayList<>());
        }
        stageToKey.get(stageName).add(joinKey);
      }
    }

    return stageToKey.entrySet().stream()
      .map(entry -> new JoinKey(entry.getKey(), entry.getValue()))
      .collect(Collectors.toSet());
  }

  List<JoinField> getSelectedFields(FailureCollector failureCollector) {
    if (Strings.isNullOrEmpty(selectedFields)) {
      failureCollector.addFailure("Must select at least one field", null).withConfigProperty(SELECTED_FIELDS);
      throw failureCollector.getOrThrowException();
    }

    List<JoinField> selectedJoinFields = new ArrayList<>();

    for (String selectedField : Splitter.on(',').trimResults().omitEmptyStrings().split(selectedFields)) {
      Iterable<String> stageOldNameAliasPair = Splitter.on(" as ").trimResults().omitEmptyStrings()
        .split(selectedField);
      Iterable<String> stageOldNamePair = Splitter.on('.').trimResults().omitEmptyStrings().
        split(Iterables.get(stageOldNameAliasPair, 0));

      if (Iterables.size(stageOldNamePair) != 2) {
        failureCollector.addFailure(String.format("Invalid syntax. Selected Fields must be of syntax " +
                                                    "<stageName>.<oldFieldName> as <alias>, but found %s",
                                                  selectedField), null)
          .withConfigProperty(SELECTED_FIELDS);
        continue;
      }

      String stageName = Iterables.get(stageOldNamePair, 0);
      String oldFieldName = Iterables.get(stageOldNamePair, 1);

      // if alias is not present in selected fields, use original field name as alias
      String alias = Iterables.size(stageOldNameAliasPair) == 1 ?
        oldFieldName : Iterables.get(stageOldNameAliasPair, 1);
      selectedJoinFields.add(new JoinField(stageName, oldFieldName, alias));
    }
    failureCollector.getOrThrowException();
    return selectedJoinFields;
  }

  Set<String> getRequiredInputs() {
    return getSet(requiredInputs);
  }

  Set<String> getBroadcastInputs() {
    return getSet(inMemoryInputs);
  }

  boolean isNullSafe() {
    return joinNullKeys == null ? true : joinNullKeys;
  }

  private Set<String> getSet(String strVal) {
    if (strVal == null || strVal.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> set = new HashSet<>();
    for (String val : Splitter.on(",").trimResults().omitEmptyStrings().split(strVal)) {
      set.add(val);
    }
    return set;
  }
}
