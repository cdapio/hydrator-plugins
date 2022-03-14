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
import io.cdap.cdap.etl.api.aggregation.GroupByAggregationDefinition;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class GroupByRelationalTest {

  @Mock
  private RelationalTranformContext relationalTranformContext;

  @Mock
  private Engine engine;

  @Mock
  private ExpressionFactory<String> expressionFactory;

  @Mock
  private Relation relation;

  @Before
  public void setUp() {
    Mockito.doReturn(engine).when(relationalTranformContext).getEngine();
    Mockito.doReturn(Optional.of(expressionFactory)).when(engine).getExpressionFactory(Mockito.any());
    Schema inputSchema = Schema.recordOf(
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("profession", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("salary", Schema.of(Schema.Type.INT))
    );
  }

  @Test
  public void testValidGroupBy() throws Exception {
    GroupByConfig config = new GroupByConfig("profession",
                                             "avgSalary: avg(salary), numEmployees: count(*)");
    GroupByAggregator aggregator = new GroupByAggregator(config);
    aggregator.transform(relationalTranformContext, relation);
    GroupByAggregationDefinition aggregationDefinition = aggregator.getAggregationDefinition();

    List<Expression> groupByExpressions = aggregationDefinition.getGroupByExpressions();
    Map<String, Expression> selectExpressions = aggregationDefinition.getSelectExpressions();

    Assert.assertEquals(1, groupByExpressions.size());
    Assert.assertEquals(3, selectExpressions.size());
  }

  @Test
  public void testMixedValidityGroupBy() {
    GroupByConfig config = new GroupByConfig("profession",
                                             "numEmployees: countif(*): condition(salary>100000)," +
                                               "stddevSal: stddev(salary)," +
                                               "maxSal: max(salary)");
    GroupByAggregator aggregator = new GroupByAggregator(config);
    Relation result = aggregator.transform(relationalTranformContext, relation);

    Assert.assertFalse(result.isValid());
    Assert.assertEquals("Unsupported aggregation definition",
                        result.getValidationError());
  }

  @Test
  public void testConditionalGroupBy() throws Exception {
    GroupByConfig config = new GroupByConfig("profession",
                                             "numEmployees: countif(*): condition(salary>100000)");
    GroupByAggregator aggregator = new GroupByAggregator(config);
    Relation result = aggregator.transform(relationalTranformContext, relation);

    Assert.assertFalse(result.isValid());
    Assert.assertEquals("Unsupported aggregation definition",
                        result.getValidationError());
  }
}
