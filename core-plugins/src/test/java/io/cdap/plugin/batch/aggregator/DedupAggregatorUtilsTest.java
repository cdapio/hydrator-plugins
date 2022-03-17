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
import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.ExtractableExpression;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Test for {@link DedupAggregatorUtils}
 */
@RunWith(MockitoJUnitRunner.class)
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
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine).getExpressionFactory(Mockito.any());


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
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine).getExpressionFactory(Mockito.any());

        DeduplicateAggregationDefinition deduplicateAggregationDefinition = DedupAggregatorUtils
                .generateAggregationDefinition(relationalTranformContext, relation,
                        filterFunction, uniqueFields);
        Assert.assertEquals(null, deduplicateAggregationDefinition);
    }

    @Test
    public void testDedupAggregatorTransformExpressionFilterFunction() {
        Mockito.doReturn(Optional.of(expressionFactory)).when(engine).getExpressionFactory(Mockito.any());
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
}
