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
 *
 */

package io.cdap.plugin.batch.aggregator;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.engine.sql.StandardSQLCapabilities;
import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.ExtractableExpression;
import io.cdap.cdap.etl.api.relational.InvalidExtractableExpressionException;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Test for {@link DedupAggregatorUtils}
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class DedupAggregatorUtilsTest {

    //Mocks used to configure tests

    @Mock
    Relation relation;

    @Mock
    private Engine engine;

    @Mock
    private ExpressionFactory<String> expressionFactory;

    @Mock
    private RelationalTranformContext relationalTranformContext;

    @Mock
    private Set<Capability> capabilities;

    @Mock
    private ExtractableExpression extractableExpression;

    private List<String> uniqueFields;

    @Before
    public void setUp() {
        Mockito.doReturn(engine).when(relationalTranformContext).getEngine();
        uniqueFields = new ArrayList<>();
        uniqueFields.add("uniqueField");
        Schema inputSchema = Schema.recordOf(
          "schema",
          Schema.Field.of("uniqueField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("uniqueFieldFloat", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
          Schema.Field.of("uniqueFieldDouble", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
          Schema.Field.of("uniqueFieldDatetime", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))),
          Schema.Field.of("selectField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("filterField", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
        );
        Mockito.doReturn(Collections.singleton("input_rel")).when(relationalTranformContext).getInputRelationNames();
        Mockito.doReturn(inputSchema).when(relationalTranformContext).getInputSchema(Mockito.eq("input_rel"));
        Schema outputSchema = Schema.recordOf(
                "schema",
                Schema.Field.of("uniqueField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("selectField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("filterField", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
        );
        Mockito.doReturn(outputSchema).when(relationalTranformContext).getOutputSchema();
    }

    @Test
    public void testDedupAggregatorTransformExpressionHappyPath() {
        DedupConfig.DedupFunctionInfo filterFunction = new DedupConfig.DedupFunctionInfo("filterField",
                                                                                         DedupConfig.Function.MIN);
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));


        Mockito.doReturn(capabilities).when(expressionFactory).getCapabilities();
        Mockito.doReturn(true).when(capabilities).contains(Mockito.any());
        Mockito.doReturn(extractableExpression).when(expressionFactory).getQualifiedColumnName(Mockito.any(),
                                                                                               Mockito.any());

        DeduplicateAggregationDefinition deduplicateAggregationDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext, relation, filterFunction, uniqueFields);
        Assert.assertEquals(3, deduplicateAggregationDefinition.getSelectExpressions().size());
        Assert.assertEquals(1, deduplicateAggregationDefinition.getGroupByExpressions().size());
        Assert.assertEquals(1, deduplicateAggregationDefinition.getFilterExpressions().size());
    }

    @Test
    public void testDedupAggregatorTransformExpressionInvalidFilterFunction() {
        DedupConfig.DedupFunctionInfo filterFunction = new DedupConfig.DedupFunctionInfo("filterField",
                                                                                         DedupConfig.Function.LAST);
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));

        DeduplicateAggregationDefinition deduplicateAggregationDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext, relation,
                                         filterFunction, uniqueFields);
        Assert.assertEquals(null, deduplicateAggregationDefinition);
    }

    @Test
    public void testDedupAggregatorTransformExpressionFilterFunction() {
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));
        Mockito.doReturn(capabilities).when(expressionFactory).getCapabilities();
        Mockito.doReturn(true).when(capabilities).contains(Mockito.any());
        Mockito.doReturn(extractableExpression).when(expressionFactory).getQualifiedColumnName(Mockito.any(),
                                                                                               Mockito.any());

        DedupConfig.DedupFunctionInfo min = new DedupConfig.DedupFunctionInfo("filterField",
                                                                              DedupConfig.Function.MIN);
        DeduplicateAggregationDefinition minDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext, relation, min, uniqueFields);
        Assert.assertEquals(DeduplicateAggregationDefinition.FilterFunction.MIN,
                            minDefinition.getFilterExpressions().get(0).getFilterFunction());

        DedupConfig.DedupFunctionInfo max = new DedupConfig.DedupFunctionInfo("filterField",
                                                                              DedupConfig.Function.MAX);
        DeduplicateAggregationDefinition maxDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext, relation, max, uniqueFields);
        Assert.assertEquals(DeduplicateAggregationDefinition.FilterFunction.MAX,
                            maxDefinition.getFilterExpressions().get(0).getFilterFunction());

        DedupConfig.DedupFunctionInfo any = new DedupConfig.DedupFunctionInfo("filterField",
                                                                              DedupConfig.Function.ANY);
        DeduplicateAggregationDefinition anyDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext, relation, any, uniqueFields);
        Assert.assertEquals(DeduplicateAggregationDefinition.FilterFunction.ANY_NULLS_LAST,
                            anyDefinition.getFilterExpressions().get(0).getFilterFunction());

        DedupConfig.DedupFunctionInfo first = new DedupConfig.DedupFunctionInfo("filterField",
                                                                                DedupConfig.Function.FIRST);
        DeduplicateAggregationDefinition firstDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext, relation, first, uniqueFields);
        Assert.assertNull(firstDefinition);

        DedupConfig.DedupFunctionInfo last = new DedupConfig.DedupFunctionInfo("filterField",
                                                                               DedupConfig.Function.LAST);
        DeduplicateAggregationDefinition lastDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext, relation, last, uniqueFields);
        Assert.assertNull(lastDefinition);
    }

    @Test
    public void testDedupAggregatorTransformExpressionWithFloatField() {
        DedupConfig.DedupFunctionInfo any = new DedupConfig.DedupFunctionInfo("filterField",
                                                                              DedupConfig.Function.ANY);
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));

        // Ensure we suppport all capabilities
        Mockito.doReturn(capabilities).when(expressionFactory).getCapabilities();
        Mockito.doReturn(true).when(capabilities).contains(Mockito.any());
        Mockito.doReturn(extractableExpression)
          .when(expressionFactory).getQualifiedColumnName(Mockito.any(), Mockito.any());

        // Configure test specific return types
        Mockito.doReturn(getExtractableExpressionForString("uniqueFieldFloat"))
          .when(expressionFactory).getQualifiedColumnName(Mockito.any(), Mockito.eq("uniqueFieldFloat"));
        Mockito.doReturn(getExtractableExpressionForString("CAST(uniqueFieldFloat AS NUMERIC)"))
          .when(expressionFactory).compile("CAST(uniqueFieldFloat AS NUMERIC)");

        DeduplicateAggregationDefinition anyDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext,
                                         relation,
                                         any,
                                         Collections.singletonList("uniqueFieldFloat"));

        // Ensure method to wrap expression got called
        Mockito.verify(expressionFactory).compile(Mockito.eq("CAST(uniqueFieldFloat AS NUMERIC)"));

        // Verify expression gets CAST statement
        Expression exp = anyDefinition.getGroupByExpressions().get(0);
        Assert.assertTrue(exp instanceof ExtractableExpression);
        ExtractableExpression<String> extractableExp = (ExtractableExpression<String>) exp;
        Assert.assertEquals("CAST(uniqueFieldFloat AS NUMERIC)", extractableExp.extract());
    }

    @Test
    public void testDedupAggregatorTransformExpressionWithDoubleField() {
        DedupConfig.DedupFunctionInfo any = new DedupConfig.DedupFunctionInfo("filterField",
                                                                              DedupConfig.Function.ANY);
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));

        // Ensure we suppport all capabilities
        Mockito.doReturn(capabilities).when(expressionFactory).getCapabilities();
        Mockito.doReturn(true).when(capabilities).contains(Mockito.any());
        Mockito.doReturn(extractableExpression)
          .when(expressionFactory).getQualifiedColumnName(Mockito.any(), Mockito.any());

        // Configure test specific return types
        Mockito.doReturn(getExtractableExpressionForString("uniqueFieldDouble"))
          .when(expressionFactory).getQualifiedColumnName(Mockito.any(), Mockito.eq("uniqueFieldDouble"));
        Mockito.doReturn(getExtractableExpressionForString("CAST(uniqueFieldDouble AS NUMERIC)"))
          .when(expressionFactory).compile("CAST(uniqueFieldDouble AS NUMERIC)");

        DeduplicateAggregationDefinition anyDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext,
                                         relation,
                                         any,
                                         Collections.singletonList("uniqueFieldDouble"));

        // Ensure method to wrap expression got called
        Mockito.verify(expressionFactory).compile(Mockito.eq("CAST(uniqueFieldDouble AS NUMERIC)"));

        // Verify expression gets CAST statement
        Expression exp = anyDefinition.getGroupByExpressions().get(0);
        Assert.assertTrue(exp instanceof ExtractableExpression);
        ExtractableExpression<String> extractableExp = (ExtractableExpression<String>) exp;
        Assert.assertEquals("CAST(uniqueFieldDouble AS NUMERIC)", extractableExp.extract());
    }

    @Test
    public void testDedupAggregatorTransformExpressionWithIntField() {
        DedupConfig.DedupFunctionInfo any = new DedupConfig.DedupFunctionInfo("filterField",
                                                                              DedupConfig.Function.ANY);
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));

        // Ensure we suppport all capabilities
        Mockito.doReturn(capabilities).when(expressionFactory).getCapabilities();
        Mockito.doReturn(true).when(capabilities).contains(Mockito.any());
        Mockito.doReturn(extractableExpression)
          .when(expressionFactory).getQualifiedColumnName(Mockito.any(), Mockito.any());

        // Configure test specific return types
        Mockito.doReturn(getExtractableExpressionForString("uniqueFieldInt"))
          .when(expressionFactory).getQualifiedColumnName(Mockito.any(), Mockito.eq("uniqueFieldInt"));

        DeduplicateAggregationDefinition anyDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext,
                                         relation,
                                         any,
                                         Collections.singletonList("uniqueFieldInt"));

        // Ensure method to wrap expression got called
        Mockito.verify(expressionFactory, Mockito.never()).compile(Mockito.any());

        // Verify expression does not get casted for an int field
        Expression exp = anyDefinition.getGroupByExpressions().get(0);
        Assert.assertTrue(exp instanceof ExtractableExpression);
        ExtractableExpression<String> extractableExp = (ExtractableExpression<String>) exp;
        Assert.assertEquals("uniqueFieldInt", extractableExp.extract());
    }

    @Test
    public void testDedupAggregatorTransformExpressionWithDatetimeField() {
        DedupConfig.DedupFunctionInfo any = new DedupConfig.DedupFunctionInfo("filterField",
                                                                              DedupConfig.Function.ANY);
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));

        // Ensure we suppport all capabilities
        Mockito.doReturn(capabilities).when(expressionFactory).getCapabilities();
        Mockito.doReturn(true).when(capabilities).contains(Mockito.any());
        Mockito.doReturn(extractableExpression)
          .when(expressionFactory).getQualifiedColumnName(Mockito.any(), Mockito.any());

        // Configure test specific return types
        Mockito.doReturn(getExtractableExpressionForString("uniqueFieldDatetime"))
          .when(expressionFactory).getQualifiedColumnName(Mockito.any(), Mockito.eq("uniqueFieldDatetime"));

        DeduplicateAggregationDefinition anyDefinition = DedupAggregatorUtils
          .generateAggregationDefinition(relationalTranformContext,
                                         relation,
                                         any,
                                         Collections.singletonList("uniqueFieldDatetime"));

        // Ensure method to wrap expression got called
        Mockito.verify(expressionFactory, Mockito.never()).compile(Mockito.any());

        // Verify expression does not get casted for an int field
        Expression exp = anyDefinition.getGroupByExpressions().get(0);
        Assert.assertTrue(exp instanceof ExtractableExpression);
        ExtractableExpression<String> extractableExp = (ExtractableExpression<String>) exp;
        Assert.assertEquals("uniqueFieldDatetime", extractableExp.extract());
    }

    @Test
    public void testGetExpressionFactoryNullSchema() {
        Optional<ExpressionFactory<String>> expressionFactory =
          DedupAggregatorUtils.getExpressionFactory(relationalTranformContext, null);
        Assert.assertFalse(expressionFactory.isPresent());
    }

    @Test
    public void testGetExpressionFactoryFloatSchema() {
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));
        Schema floatSchema =
          Schema.recordOf(
            "schema",
            Schema.Field.of("uniqueFieldFloat", Schema.of(Schema.Type.FLOAT)));

        Optional<ExpressionFactory<String>> expressionFactory =
          DedupAggregatorUtils.getExpressionFactory(relationalTranformContext, floatSchema);

        Assert.assertTrue(expressionFactory.isPresent());
        Mockito.verify(engine, Mockito.times(1))
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));
        Mockito.verify(engine, Mockito.never())
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL));
    }

    @Test
    public void testGetExpressionFactoryNullableFloatSchema() {
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));
        Schema nullableFloatSchema =
          Schema.recordOf(
            "schema",
            Schema.Field.of("uniqueFieldFloat", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))));

        Optional<ExpressionFactory<String>> expressionFactory =
          DedupAggregatorUtils.getExpressionFactory(relationalTranformContext, nullableFloatSchema);

        Assert.assertTrue(expressionFactory.isPresent());
        Mockito.verify(engine, Mockito.times(1))
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));
        Mockito.verify(engine, Mockito.never())
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL));
    }

    @Test
    public void testGetExpressionFactoryDoubleSchema() {
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));

        Schema doubleSchema =
          Schema.recordOf(
            "schema",
            Schema.Field.of("uniqueFieldDouble", Schema.of(Schema.Type.DOUBLE)));

        Optional<ExpressionFactory<String>> expressionFactory =
          DedupAggregatorUtils.getExpressionFactory(relationalTranformContext, doubleSchema);

        Assert.assertTrue(expressionFactory.isPresent());
        Mockito.verify(engine, Mockito.times(1))
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));
        Mockito.verify(engine, Mockito.never())
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL));
    }

    @Test
    public void testGetExpressionFactoryNullableDoubleSchema() {
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));
        Schema nullableDoubleSchema =
          Schema.recordOf(
            "schema",
            Schema.Field.of("uniqueFieldDouble", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

        Optional<ExpressionFactory<String>> expressionFactory =
          DedupAggregatorUtils.getExpressionFactory(relationalTranformContext, nullableDoubleSchema);

        Assert.assertTrue(expressionFactory.isPresent());
        Mockito.verify(engine, Mockito.times(1))
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));
        Mockito.verify(engine, Mockito.never())
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL));
    }

    @Test
    public void testGetExpressionFactoryNotNullableOrDoubleSchema() {
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine)
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL));
        Schema intSchema =
          Schema.recordOf(
            "schema",
            Schema.Field.of("uniqueFieldInt", Schema.nullableOf(Schema.of(Schema.Type.INT))));

        Optional<ExpressionFactory<String>> expressionFactory =
          DedupAggregatorUtils.getExpressionFactory(relationalTranformContext, intSchema);

        Assert.assertTrue(expressionFactory.isPresent());
        Mockito.verify(engine, Mockito.never())
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL),
                                Mockito.eq(StandardSQLCapabilities.BIGQUERY));
        Mockito.verify(engine, Mockito.times(1))
          .getExpressionFactory(Mockito.eq(StringExpressionFactoryType.SQL));
    }

    private static ExtractableExpression<String> getExtractableExpressionForString(String str) {
        return new ExtractableExpression<String>() {
            @Nullable
            @Override
            public String extract() throws InvalidExtractableExpressionException {
                return String.format("%s", str);
            }

            @Override
            public boolean isValid() {
                return true;
            }

            @Override
            public String getValidationError() {
                return null;
            }
        };
    }
}
