/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.batch.aggregator;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.engine.sql.StandardSQLCapabilities;
import io.cdap.cdap.etl.api.relational.CoreExpressionCapabilities;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.ExtractableExpression;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Utility class for DedupAggregator.
 */
public class DedupAggregatorUtils {

  private static final String CAST_TO_NUMERIC_FORMAT = "CAST(%s AS NUMERIC)";

  /**
   * Generates a {@link DeduplicateAggregationDefinition} based on an input relation and deduplication configuration.
   *
   * @param ctx          transform context
   * @param relation     input relation
   * @param uniqueFields unique fields when deduplicating
   * @param filter       filter function
   * @return Definition for a deduplicate operation, or null if this deduplicate definition is not supported.
   */
  @Nullable
  public static DeduplicateAggregationDefinition generateAggregationDefinition(
    RelationalTranformContext ctx,
    Relation relation,
    List<String> uniqueFields,
    @Nullable DedupConfig.DedupFunctionInfo filter) {
    // Deduplication contain only one input schema.
    String inputRelationName = ctx.getInputRelationNames().stream().findFirst().orElse(null);
    Schema inputSchema = inputRelationName != null ? ctx.getInputSchema(inputRelationName) : null;

    Optional<ExpressionFactory<String>> expressionFactory = getExpressionFactory(ctx, inputSchema);

    // If the expression factory is not present, this aggregation cannot be handled by the plugin.
    if (!expressionFactory.isPresent()) {
      return null;
    }

    // Get String Expression Factory
    ExpressionFactory<String> stringExpressionFactory = expressionFactory.get();

    // Filter function will be null if not specified in the plugin settings
    Expression filterExpression = null;
    DeduplicateAggregationDefinition.FilterFunction aggFilterFunction = null;
    if (filter != null) {
      // Determine which filtering function to use
      switch (filter.getFunction()) {
        case MAX:
          aggFilterFunction = DeduplicateAggregationDefinition.FilterFunction.MAX;
          break;
        case MIN:
          aggFilterFunction = DeduplicateAggregationDefinition.FilterFunction.MIN;
          break;
        case ANY:
          aggFilterFunction = DeduplicateAggregationDefinition.FilterFunction.ANY_NULLS_LAST;
          break;
        default:
          // Aggregation is not supported in BigQuery
          return null;
      }

      // Build filter expression
      filterExpression = getColumnName(relation, filter.getField(), stringExpressionFactory);
    }

    DeduplicateAggregationDefinition.Builder builder = DeduplicateAggregationDefinition.builder();

    // Build select fields
    Map<String, Expression> selectExpressions = new HashMap<>();
    for (Schema.Field field : ctx.getOutputSchema().getFields()) {
      selectExpressions.put(field.getName(), getColumnName(relation, field.getName(), stringExpressionFactory));
    }
    builder.select(selectExpressions);

    // Specify filters to deduplicate on
    List<Expression> dedupExpressions = new ArrayList<>();
    for (String uniqueField : uniqueFields) {
      dedupExpressions.add(getDedupColumnName(relation, uniqueField, stringExpressionFactory, inputSchema));
    }
    builder.dedupOn(dedupExpressions);

    // Add aggregation filter function if specified
    if (aggFilterFunction != null) {
      builder.filterDuplicatesBy(filterExpression, aggFilterFunction);
    }

    // return dedup definition
    return builder.build();
  }

  static Expression getColumnName(Relation relation,
                                  String name,
                                  ExpressionFactory<String> stringExpressionFactory) {
    if (stringExpressionFactory.getCapabilities()
      .contains(CoreExpressionCapabilities.CAN_GET_QUALIFIED_COLUMN_NAME)) {
      return stringExpressionFactory.getQualifiedColumnName(relation, name);
    } else {
      return stringExpressionFactory.compile(name);
    }
  }

  static Expression getDedupColumnName(Relation relation,
                                       String name,
                                       ExpressionFactory<String> stringExpressionFactory,
                                       @Nullable Schema schema) {
    Expression columnNameExp = getColumnName(relation, name, stringExpressionFactory);

    // Check if the field is a Float or a Double, as we need to cast the expression to NUMERIC in order to dedup on
    // this field.
    if (columnNameExp instanceof ExtractableExpression && schema != null && schema.getField(name) != null) {
      if (isFloatOrDoubleField(schema.getField(name))) {
        String castExp = String.format(CAST_TO_NUMERIC_FORMAT, ((ExtractableExpression<?>) columnNameExp).extract());
        columnNameExp = stringExpressionFactory.compile(castExp);
      }
    }


    return columnNameExp;
  }

  @VisibleForTesting
  protected static Optional<ExpressionFactory<String>> getExpressionFactory(RelationalTranformContext ctx,
                                                                            @Nullable Schema inputSchema) {
    // We need a valid input schema to decide which capabilities are required.
    if (inputSchema == null || inputSchema.getFields() == null) {
      return Optional.empty();
    }

    // The BigQuery capability is required for Float or Double fields.
    boolean requiresBigQueryCapability =
      inputSchema.getFields().stream().anyMatch(DedupAggregatorUtils::isFloatOrDoubleField);

    // If the BigQuery capability is required, ensure the SQL engine supports this capability.
    return requiresBigQueryCapability ?
      ctx.getEngine().getExpressionFactory(StringExpressionFactoryType.SQL, StandardSQLCapabilities.BIGQUERY) :
      ctx.getEngine().getExpressionFactory(StringExpressionFactoryType.SQL);
  }

  @VisibleForTesting
  protected static boolean isFloatOrDoubleField(Schema.Field field) {
    Schema fieldSchema = field.getSchema();

    // Get the non-nullable schema for this field
    if (fieldSchema.isNullable()) {
      fieldSchema = fieldSchema.getNonNullable();
    }

    // If the type is Float or Double, ensure we cast it to numeric.
    if (fieldSchema.getLogicalType() == null
      && (fieldSchema.getType() == Schema.Type.FLOAT || fieldSchema.getType() == Schema.Type.DOUBLE)) {
      return true;
    }

    return false;
  }
}
