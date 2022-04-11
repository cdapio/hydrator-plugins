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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
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

  public static DeduplicateAggregationDefinition generateAggregationDefinition(RelationalTranformContext ctx,
                                                                               Relation relation,
                                                                               DedupConfig.DedupFunctionInfo filter,
                                                                               List<String> uniqueFields) {
    // Deduplication contain only one input schema.
    String inputRelationName = ctx.getInputRelationNames().stream().findFirst().orElse(null);
    Schema inputSchema = inputRelationName != null ? ctx.getInputSchema(inputRelationName) : null;

    Optional<ExpressionFactory<String>> expressionFactory = ctx.getEngine().
      getExpressionFactory(StringExpressionFactoryType.SQL);
    DeduplicateAggregationDefinition.FilterFunction aggFilterFunction;
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
        return null;
    }

    Map<String, Expression> selectExpressions = new HashMap<>();
    List<Expression> dedupExpressions = new ArrayList<>();
    ExpressionFactory<String> stringExpressionFactory = expressionFactory.get();

    Expression filterExpression = getColumnName(relation, filter.getField(), stringExpressionFactory);

    for (Schema.Field field : ctx.getOutputSchema().getFields()) {
      selectExpressions.put(field.getName(), getColumnName(relation, field.getName(), stringExpressionFactory));
    }


    for (String uniqueField : uniqueFields) {
      dedupExpressions.add(getDedupColumnName(relation, uniqueField, stringExpressionFactory, inputSchema));
    }

    return DeduplicateAggregationDefinition.builder()
      .select(selectExpressions)
      .dedupOn(dedupExpressions)
      .filterDuplicatesBy(filterExpression, aggFilterFunction)
      .build();
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
      Schema fieldSchema = schema.getField(name).getSchema();

      // Get the non-nullable schema for this field
      if (fieldSchema != null) {
        if (fieldSchema.isNullable()) {
          fieldSchema = fieldSchema.getNonNullable();
        }

        // If the type is Float or Double, ensure we cast it to numeric.
        if (fieldSchema.getLogicalType() == null
          && (fieldSchema.getType() == Schema.Type.FLOAT || fieldSchema.getType() == Schema.Type.DOUBLE)) {
          String castExp = String.format(CAST_TO_NUMERIC_FORMAT, ((ExtractableExpression<?>) columnNameExp).extract());
          columnNameExp = stringExpressionFactory.compile(castExp);
        }
      }
    }

    return columnNameExp;
  }
}
