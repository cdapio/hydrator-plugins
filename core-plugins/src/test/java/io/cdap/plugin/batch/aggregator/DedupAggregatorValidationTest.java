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
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class DedupAggregatorValidationTest {
    @Mock
    private PipelineConfigurer pipelineConfigurer;

    @Mock
    private StageConfigurer stageConfigurer;

    @Mock
    private DedupConfig dedupConfig;

    private DedupConfig.DedupFunctionInfo functionInfo;
    private List<String> uniqueFields;
    private MockFailureCollector collector;

    @Before
    public void setUp() {
        collector = new MockFailureCollector();
        Schema stringSchema = Schema.of(Schema.Type.STRING);
        Schema intSchema = Schema.of(Schema.Type.INT);
        Schema inputSchema = Schema.recordOf(
                Schema.Field.of("name", Schema.nullableOf(stringSchema)),
                Schema.Field.of("profession", Schema.nullableOf(stringSchema)),
                Schema.Field.of("age", Schema.nullableOf(intSchema))
        );
        uniqueFields = new ArrayList<>();
        uniqueFields.add("name");
        Mockito.doReturn(uniqueFields).when(dedupConfig).getUniqueFields();
        Mockito.doReturn(stageConfigurer).when(pipelineConfigurer).getStageConfigurer();
        Mockito.doReturn(collector).when(stageConfigurer).getFailureCollector();
        Mockito.doReturn(inputSchema).when(stageConfigurer).getInputSchema();
    }

    @Test
    public void testValidateHappyPathNonNumericFilterField() {
        functionInfo = new DedupConfig.DedupFunctionInfo("age", DedupConfig.Function.MIN);
        Mockito.doReturn(functionInfo).when(dedupConfig).getFilter();
        DedupAggregator dedupAggregator = new DedupAggregator(dedupConfig);
        dedupAggregator.configurePipeline(pipelineConfigurer);
        Assert.assertEquals(collector.getValidationFailures().size(), 0);
    }

    @Test
    public void testValidationExceptionNonNumericFilterField() {
        functionInfo = new DedupConfig.DedupFunctionInfo("profession", DedupConfig.Function.MIN);
        Mockito.doReturn(functionInfo).when(dedupConfig).getFilter();
        DedupAggregator dedupAggregator = new DedupAggregator(dedupConfig);
        dedupAggregator.configurePipeline(pipelineConfigurer);
        Assert.assertEquals(collector.getValidationFailures().size(), 1);
        Assert.assertEquals(collector.getValidationFailures().get(0).getMessage(), "Unsupported filter " +
                "operation MIN(profession): Field 'profession' is non numeric ");
    }
}
