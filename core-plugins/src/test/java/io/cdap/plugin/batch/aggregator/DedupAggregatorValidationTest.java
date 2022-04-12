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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class DedupAggregatorValidationTest {
    @Mock
    private PipelineConfigurer pipelineConfigurer;

    @Mock
    private StageConfigurer stageConfigurer;

    @Mock
    private BatchRuntimeContext context;

    @Mock
    private DedupConfig dedupConfig;

    private DedupConfig.DedupFunctionInfo functionInfo;
    private List<String> uniqueFields;
    private MockFailureCollector collector;
    private Schema inputSchema;
    private StructuredRecord aggValue, record;

    @Before
    public void setUp() {
        collector = new MockFailureCollector();
        Schema stringSchema = Schema.of(Schema.Type.STRING);
        Schema booleanSchema = Schema.of(Schema.Type.BOOLEAN);
        Schema decimalSchema = Schema.decimalOf(6, 2);
        Schema dateTimeSchema = Schema.of(Schema.LogicalType.DATETIME);
        Schema dateSchema = Schema.of(Schema.LogicalType.DATE);
        Schema timeSchema = Schema.of(Schema.LogicalType.TIME_MILLIS);
        Schema timeStampSchema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        Schema floatSchema = Schema.of(Schema.Type.FLOAT);
        Schema intSchema = Schema.of(Schema.Type.INT);
        inputSchema = Schema.recordOf(
                Schema.Field.of("name", Schema.nullableOf(stringSchema)),
                Schema.Field.of("hasInsurance", Schema.nullableOf(booleanSchema)),
                Schema.Field.of("price", Schema.nullableOf(decimalSchema)),
                Schema.Field.of("date and time of purchase", Schema.nullableOf(dateTimeSchema)),
                Schema.Field.of("date of purchase", Schema.nullableOf(dateSchema)),
                Schema.Field.of("time of purchase", Schema.nullableOf(timeSchema)),
                Schema.Field.of("timestamp", Schema.nullableOf(timeStampSchema)),
                Schema.Field.of("height", Schema.nullableOf(floatSchema)),
                Schema.Field.of("email", Schema.nullableOf(stringSchema)),
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
    public void testValidationExceptionStringFilterField() {
        functionInfo = new DedupConfig.DedupFunctionInfo("email", DedupConfig.Function.MIN);
        Mockito.doReturn(functionInfo).when(dedupConfig).getFilter();
        DedupAggregator dedupAggregator = new DedupAggregator(dedupConfig);
        dedupAggregator.configurePipeline(pipelineConfigurer);
        Assert.assertEquals(collector.getValidationFailures().size(), 1);
        Assert.assertEquals(collector.getValidationFailures().get(0).getMessage(), "Unsupported filter " +
                "operation MIN(email): Field has a type that is not supported for deduplication operations");
    }

    @Test
    public void testValidationExceptionNonNumericFilterField() {
        functionInfo = new DedupConfig.DedupFunctionInfo("hasInsurance", DedupConfig.Function.MIN);
        Mockito.doReturn(functionInfo).when(dedupConfig).getFilter();
        DedupAggregator dedupAggregator = new DedupAggregator(dedupConfig);
        dedupAggregator.configurePipeline(pipelineConfigurer);
        Assert.assertEquals(collector.getValidationFailures().size(), 1);
        Assert.assertEquals(collector.getValidationFailures().get(0).getMessage(), "Unsupported filter " +
                "operation MIN(hasInsurance): Field has a type that is not supported for deduplication operations");
    }

    @Test
    public void testSelectFilterFunctionFieldDateTime() {
        aggValue = StructuredRecord.builder(inputSchema).setDateTime("date and time of purchase",
                LocalDateTime.of(2015,
                        Month.JULY, 29, 19, 30, 40)).build();
        record  = StructuredRecord.builder(inputSchema).setDateTime("date and time of purchase",
                LocalDateTime.of(2017,
                        Month.JULY, 29, 19, 30, 40)).build();

        functionInfo = new DedupConfig.DedupFunctionInfo("date and time of purchase", DedupConfig.Function.MIN);
        Mockito.doReturn(uniqueFields).when(dedupConfig).getUniqueFields();
        Mockito.doReturn(functionInfo).when(dedupConfig).getFilter();
        DedupAggregator dedupAggregator = new DedupAggregator(dedupConfig);
        dedupAggregator.initialize(context);
        StructuredRecord actualRes = dedupAggregator.mergeValues(aggValue, record);
        Assert.assertEquals(actualRes.getDateTime("date and time of purchase"), LocalDateTime.of(2015,
                Month.JULY, 29, 19, 30, 40));
    }

    @Test
    public void testSelectFilterFunctionFieldDecimal() {
        aggValue = StructuredRecord.builder(inputSchema).setDecimal("price",
                new BigDecimal("4000.89")).build();
        record  = StructuredRecord.builder(inputSchema).setDecimal("price",
                new BigDecimal("4000.91")).build();

        functionInfo = new DedupConfig.DedupFunctionInfo("price", DedupConfig.Function.MAX);
        Mockito.doReturn(uniqueFields).when(dedupConfig).getUniqueFields();
        Mockito.doReturn(functionInfo).when(dedupConfig).getFilter();
        DedupAggregator dedupAggregator = new DedupAggregator(dedupConfig);
        dedupAggregator.initialize(context);
        StructuredRecord actualRes = dedupAggregator.mergeValues(aggValue, record);
        Assert.assertEquals(actualRes.getDecimal("price"), new BigDecimal("4000.91"));
    }

    @Test
    public void testSelectFilterFunctionFieldDate() {
        aggValue = StructuredRecord.builder(inputSchema).setDate("date of purchase",
                LocalDate.of(2020, 4, 23)).build();
        record  = StructuredRecord.builder(inputSchema).setDate("date of purchase",
                LocalDate.of(2002, 4, 23)).build();

        functionInfo = new DedupConfig.DedupFunctionInfo("date of purchase", DedupConfig.Function.MIN);
        Mockito.doReturn(uniqueFields).when(dedupConfig).getUniqueFields();
        Mockito.doReturn(functionInfo).when(dedupConfig).getFilter();
        DedupAggregator dedupAggregator = new DedupAggregator(dedupConfig);
        dedupAggregator.initialize(context);
        StructuredRecord actualRes = dedupAggregator.mergeValues(aggValue, record);
        Assert.assertEquals(actualRes.getDate("date of purchase"),
                LocalDate.of(2002, 4, 23));
    }

    @Test
    public void testSelectFilterFunctionFieldTimestamp() {
        ZonedDateTime aggValueTimeObj = ZonedDateTime.of(2018, 01,
                01, 0, 0, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime recordTimeObj = ZonedDateTime.of(2022, 01,
                01, 0, 0, 0, 0, ZoneId.of("UTC"));
        aggValue = StructuredRecord.builder(inputSchema).setTimestamp("timestamp",
                aggValueTimeObj).build();
        record  = StructuredRecord.builder(inputSchema).setTimestamp("timestamp",
                recordTimeObj).build();

        functionInfo = new DedupConfig.DedupFunctionInfo("timestamp", DedupConfig.Function.MAX);
        Mockito.doReturn(uniqueFields).when(dedupConfig).getUniqueFields();
        Mockito.doReturn(functionInfo).when(dedupConfig).getFilter();
        DedupAggregator dedupAggregator = new DedupAggregator(dedupConfig);
        dedupAggregator.initialize(context);
        StructuredRecord actualRes = dedupAggregator.mergeValues(aggValue, record);
        Assert.assertEquals(actualRes.getTimestamp("timestamp"), recordTimeObj);
    }

    @Test
    public void testSelectFilterFunctionFieldTime() {
        aggValue = StructuredRecord.builder(inputSchema).setTime("time of purchase",
                LocalTime.of(10, 43, 12)).build();
        record  = StructuredRecord.builder(inputSchema).setTime("time of purchase", 
                LocalTime.of(12, 43 , 12)).build();

        functionInfo = new DedupConfig.DedupFunctionInfo("time of purchase", DedupConfig.Function.MAX);
        Mockito.doReturn(uniqueFields).when(dedupConfig).getUniqueFields();
        Mockito.doReturn(functionInfo).when(dedupConfig).getFilter();
        DedupAggregator dedupAggregator = new DedupAggregator(dedupConfig);
        dedupAggregator.initialize(context);
        StructuredRecord actualRes = dedupAggregator.mergeValues(aggValue, record);
        Assert.assertEquals(actualRes.getTime("time of purchase"),
                LocalTime.of(12, 43, 12));
    }

    @Test
    public void testSelectFilterFunctionFieldFloat() {
        functionInfo = new DedupConfig.DedupFunctionInfo("height", DedupConfig.Function.MAX);
        Mockito.doReturn(functionInfo).when(dedupConfig).getFilter();
        DedupAggregator dedupAggregator = new DedupAggregator(dedupConfig);
        dedupAggregator.configurePipeline(pipelineConfigurer);
        Assert.assertEquals(collector.getValidationFailures().size(), 0);
    }
}
