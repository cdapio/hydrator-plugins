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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.plugin.common.KeyValueListParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

  public static final String CONDITION_TYPE = "conditionType";
  public static final String CONDITION_EXPR = "conditionExpression";
  public static final String DISTRIBUTION_ENABLED = "distributionEnabled";
  public static final String DISTRIBUTION_FACTOR = "distributionFactor";
  public static final String DISTRIBUTION_STAGE = "distributionStageName";
  public static final String INPUT_ALIASES = "inputAliases";
  public static final String JOIN_KEYS = "joinKeys";
  public static final String JOIN_NULL_KEYS = "joinNullKeys";
  public static final String MEMORY_INPUTS = "inMemoryInputs";
  public static final String NUM_PARTITIONS = "numPartitions";
  public static final String REQUIRED_INPUTS = "requiredInputs";
  public static final String SELECTED_FIELDS = "selectedFields";
  public static final String OUTPUT_SCHEMA = "schema";
  private static final String BASIC = "basic";
  private static final String ADVANCED = "advanced";

  @Macro
  @Nullable
  @Name(NUM_PARTITIONS)
  @Description("Number of partitions to use when joining. " +
    "If not specified, the execution framework will decide how many to use.")
  protected Integer numPartitions;

  @Macro
  @Nullable
  @Name(JOIN_KEYS)
  @Description("List of join keys to perform join operation. The list is " +
    "separated by '&'. Join key from each input stage will be prefixed with '<stageName>.' And the " +
    "relation among join keys from different inputs is represented by '='. For example: " +
    "customers.customer_id=items.c_id&customers.customer_name=items.c_name means the join key is a composite key" +
    " of customer id and customer name from customers and items input stages and join will be performed on equality " +
    "of the join keys.")
  protected String joinKeys;

  @Macro
  @Name(SELECTED_FIELDS)
  @Description("Comma-separated list of fields to be selected and renamed " +
    "in join output from each input stage. Each selected input field name needs to be present in the output must be " +
    "prefixed with '<input_stage_name>'. The syntax for specifying alias for each selected field is similar to sql. " +
    "For example: customers.id as customer_id, customer.name as customer_name, item.id as item_id, " +
    "<stageName>.inputFieldName as alias. The output will have same order of fields as selected in selectedFields." +
    "There must not be any duplicate fields in output.")
  protected String selectedFields;

  @Macro
  @Nullable
  @Name(REQUIRED_INPUTS)
  @Description("Comma-separated list of stages." +
    " Required input stages decide the type of the join. If all the input stages are present in required inputs, " +
    "inner join will be performed. Otherwise, outer join will be performed considering non-required inputs as " +
    "optional.")
  protected String requiredInputs;

  @Macro
  @Nullable
  @Name(OUTPUT_SCHEMA)
  @Description(OUTPUT_SCHEMA)
  private String schema;

  @Macro
  @Nullable
  @Name(MEMORY_INPUTS)
  @Description("Set of input stages to try to load completely in memory to perform an in-memory join. " +
    "This is just a hint to the underlying execution engine to try and perform an in-memory join. " +
    "Whether it is actually loaded into memory is up to the engine. This property is ignored when MapReduce is used.")
  private String inMemoryInputs;

  @Macro
  @Nullable
  @Name(JOIN_NULL_KEYS)
  @Description("Whether null values in the join key should be joined on. For example, if the join is on A.id = B.id " +
    "and this value is false, records with a null id from input stages A and B will not get joined together.")
  private Boolean joinNullKeys;

  @Macro
  @Nullable
  @Name(DISTRIBUTION_FACTOR)
  @Description("This controls the size of the salt that will be generated for distribution. The number of partitions "
    + "should be greater than or equal to this number for optimal results. A larger value will lead to more "
    + "parallelism but it will also grow the size of the non-skewed dataset by this factor.")
  private Integer distributionFactor;

  @Macro
  @Nullable
  @Name(DISTRIBUTION_STAGE)
  @Description("Name of the skewed input stage. The skewed input stage is the one that contains many rows that join "
    + "to the same row in the non-skewed stage. Ex. If stage A has 10 rows that join on the same row in stage B, then"
    + " stage A is the skewed input stage")
  private String distributionStageName;

  @Macro
  @Nullable
  @Name(DISTRIBUTION_ENABLED)
  @Description("Distribution is useful when the input data is skewed. Enabling distribution will allow you to "
    + "increase the parallelism for skewed data.")
  private Boolean distributionEnabled;

  @Macro
  @Nullable
  @Name(CONDITION_TYPE)
  @Description("Whether to join on equality or a more complex condition.")
  private String conditionType;

  @Macro
  @Nullable
  @Name(CONDITION_EXPR)
  @Description("Join condition as a SQL expression.")
  private String conditionExpression;

  @Macro
  @Nullable
  @Name(INPUT_ALIASES)
  @Description("Aliases for input data. " +
    "Use this to give more readable names to input data when using advanced join conditions.")
  private String inputAliases;

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

  @VisibleForTesting
  JoinerConfig(String selectedFields, String conditionExpression) {
    this.selectedFields = selectedFields;
    this.requiredInputs = requiredInputs;
    this.conditionType = ADVANCED;
    this.conditionExpression = conditionExpression;
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
    return containsMacro(SELECTED_FIELDS) || containsMacro(REQUIRED_INPUTS) ||
      containsMacro(OUTPUT_SCHEMA) || containsMacro(CONDITION_TYPE) ||
      (BASIC.equalsIgnoreCase(conditionType) && containsMacro(JOIN_KEYS)) ||
      (ADVANCED.equalsIgnoreCase(conditionType) && containsMacro(CONDITION_EXPR));
  }

  private JoinCondition.Op getConditionType(FailureCollector failureCollector) {
    if (conditionType == null || conditionType.isEmpty() || BASIC.equals(conditionType)) {
      return JoinCondition.Op.KEY_EQUALITY;
    }
    if (ADVANCED.equalsIgnoreCase(conditionType)) {
      return JoinCondition.Op.EXPRESSION;
    }
    failureCollector.addFailure("Invalid condition type " + conditionType, "Set it to 'basic' or 'advanced'.");
    throw failureCollector.getOrThrowException();
  }

  private Map<String, String> getInputAliases(FailureCollector failureCollector) {
    if (inputAliases == null || inputAliases.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> aliases = new HashMap<>();
    KeyValueListParser kvParser = new KeyValueListParser(";", "=");
    try {
      for (KeyValue<String, String> alias : kvParser.parse(inputAliases)) {
        aliases.put(alias.getKey(), alias.getValue());
      }
    } catch (Exception e) {
      failureCollector.addFailure(e.getMessage(), null).withConfigProperty(INPUT_ALIASES);
    }
    return aliases;
  }

  JoinCondition getCondition(FailureCollector failureCollector) {
    JoinCondition.Op conditionType = getConditionType(failureCollector);
    switch (conditionType) {
      case KEY_EQUALITY:
        return JoinCondition.onKeys()
          .setKeys(getJoinKeys(failureCollector))
          .setNullSafe(isNullSafe())
          .build();
      case EXPRESSION:
        if (conditionExpression == null || conditionExpression.isEmpty()) {
          failureCollector.addFailure("A join condition must be specified.", null)
            .withConfigProperty(CONDITION_EXPR);
          throw failureCollector.getOrThrowException();
        }
        return JoinCondition.onExpression()
          .setExpression(conditionExpression)
          .setDatasetAliases(getInputAliases(failureCollector))
          .build();
    }
    // will never happen unless getConditionType() is changed without changing this
    throw new IllegalStateException("Unsupported condition type " + conditionType);
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

  @Nullable
  public Integer getDistributionFactor() {
    return distributionFactor;
  }

  @Nullable
  public String getDistributionStageName() {
    return distributionStageName;
  }

  public boolean isDistributionValid(FailureCollector collector) {
    int startFailures = collector.getValidationFailures().size();

    //If distribution is disabled then no need to validate further
    if (!containsMacro("distributionEnabled") && (distributionEnabled == null || !distributionEnabled)) {
      return false;
    }

    if (!containsMacro(DISTRIBUTION_FACTOR) && distributionFactor == null) {
      collector.addFailure("Distribution Size is a required value if distribution is enabled.", "")
        .withConfigProperty(DISTRIBUTION_FACTOR);
    }
    if (!containsMacro(DISTRIBUTION_STAGE) && Strings.isNullOrEmpty(distributionStageName)) {
      collector.addFailure("Skewed Stage name is a required value if distribution is enabled.", "")
        .withConfigProperty(DISTRIBUTION_STAGE);
    }

    // If there are still macro values then this config is not valid
    if (distributionContainsMacro()) {
      return false;
    }

    return startFailures == collector.getValidationFailures().size();
  }

  public boolean distributionContainsMacro() {
    return containsMacro("distributionEnabled") ||
      containsMacro(DISTRIBUTION_FACTOR) ||
      containsMacro(DISTRIBUTION_STAGE);
  }
}
