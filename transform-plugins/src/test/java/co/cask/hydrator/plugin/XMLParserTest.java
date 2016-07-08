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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.hydrator.common.MockPipelineConfigurer;
import co.cask.hydrator.common.test.MockEmitter;
import co.cask.hydrator.common.test.MockTransformContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class XMLParserTest extends TransformPluginsTestBase {
  private static final Schema INPUT = Schema.recordOf("input", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  private static final Logger LOG = LoggerFactory.getLogger(XMLParserTest.class);

  @Test
  public void testInvalidConfig() throws Exception {
    XMLParser.Config config = new XMLParser.Config("body", "UTF-8", "category://book/@category,title://book/title," +
      "year:/bookstore/book[price>35.00]/year,price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory",
                                                   "category:,title:string,price:double,year:int,subcategory:string",
                                                   "Exit on error");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(INPUT);
    try {
      new XMLParser(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Type cannot be null. Please specify type for category", e.getMessage());
    }
  }

  @Test
  public void testXMLParserWithSimpleXPath() throws Exception {
    String inputTable = "input-simple-xpath";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));

    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put("input", "body")
      .put("encoding", "UTF-8")
      .put("xPathMappings", "title:/book/title,author:/book/author,year:/book/year")
      .put("fieldTypeMapping", "title:string,author:string,year:string")
      .put("processOnError", "Write to error dataset")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("XMLParser", Transform.PLUGIN_TYPE, sourceProperties, null));
    String sinkTable = "output-simple-xpath";

    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("XMLReaderTest");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title>" +
          "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book>").build()
    );
    MockSource.writeInput(inputManager, input);
    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals("OutputRecords", 1, outputRecords.size());
    StructuredRecord record = outputRecords.get(0);
    Assert.assertEquals("Everyday Italian", record.get("title"));
    Assert.assertEquals("Giada De Laurentiis", record.get("author"));
  }

  @Test
  public void testXpathWithMultipleElements() throws Exception {
    String inputTable = "input-xpath-with-multiple-elements";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put("input", "body")
      .put("encoding", "UTF-16 (Unicode with byte-order mark)")
      .put("xPathMappings", "category://book/@category,title://book/title,year:/bookstore/book[price>35.00]/year," +
        "price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory")
      .put("fieldTypeMapping", "category:string,title:string,price:double,year:int,subcategory:string")
      .put("processOnError", "Ignore error and continue")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("XMLParser", Transform.PLUGIN_TYPE, sourceProperties, null));
    String sinkTable = "output-xpath-with-multiple-elements";

    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("XMLReaderTest");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "<bookstore><book category=\"cooking\"><subcategory><type>Continental</type></subcategory>" +
          "<title lang=\"en\">Everyday Italian</title><author>Giada De Laurentiis</author><year>2005</year>" +
          "<price>30.00</price></book></bookstore>").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 2)
        .set("body", "<bookstore><book category=\"children\"><subcategory><type>Series</type></subcategory>" +
          "<title lang=\"en\">Harry Potter</title><author>J K. Rowling</author><year>2005</year><price>49.99</price>" +
          "</book></bookstore>").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 3)
        .set("body", "<bookstore><book category=\"web\"><subcategory><type>Basics</type></subcategory>" +
          "<title lang=\"en\">Learning XML</title><author>Erik T. Ray</author><year>2003</year><price>39.95</price>" +
          "</book></bookstore>").build()
    );
    MockSource.writeInput(inputManager, input);
    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals("OutputRecords", 3, outputRecords.size());
    for (StructuredRecord record : outputRecords) {
      if (record.get("category").equals("cooking")) {
        Assert.assertEquals("Everyday Italian", record.get("title"));
        Assert.assertEquals(null, record.get("price"));
        Assert.assertEquals(null, record.get("year"));
        Assert.assertEquals("<subcategory><type>Continental</type></subcategory>", record.get("subcategory"));
      } else if (record.get("category").equals("children")) {
        Assert.assertEquals("Harry Potter", record.get("title"));
        Assert.assertEquals(49.99, (Double) record.get("price"), 0.0);
        Assert.assertEquals(2005, record.get("year"));
        Assert.assertEquals("<subcategory><type>Series</type></subcategory>", record.get("subcategory"));
      } else if (record.get("category").equals("web")) {
        Assert.assertEquals("Learning XML", record.get("title"));
        Assert.assertEquals(39.95, (Double) record.get("price"), 0.01);
        Assert.assertEquals(2003, record.get("year"));
        Assert.assertEquals("<subcategory><type>Basics</type></subcategory>", record.get("subcategory"));
      }
    }
  }

  @Test
  public void testEmitErrors() throws Exception {
    XMLParser.Config config = new XMLParser.Config("body", "UTF-8", "title:/book/title,author:/book/author," +
      "year:/book/year", "title:string,author:int,year:int", "Write to error dataset");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
    transform.initialize(new MockTransformContext());

    StructuredRecord inputRecord = StructuredRecord.builder(INPUT)
      .set("offset", 1)
      .set("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title>" +
        "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book>").build();

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    transform.transform(inputRecord, emitter);
    Assert.assertEquals(0, emitter.getEmitted().size());
    Assert.assertEquals(1, emitter.getErrors().size());

    InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
    Assert.assertEquals(31, invalidEntry.getErrorCode());
    Assert.assertEquals("offset", 1, invalidEntry.getInvalidRecord().get("offset"));
    Assert.assertEquals("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title><author>Giada " +
      "De Laurentiis</author><year>2005</year><price>30.00</price>" +
      "</book>", invalidEntry.getInvalidRecord().get("body"));
  }

  @Test
  public void testIgnoreError() throws Exception {
    XMLParser.Config config = new XMLParser.Config("body", "ISO-8859-1", "title:/book/title,author:/book/author," +
      "year:/book/year", "title:string,author:string,year:int", "Ignore error and continue");
    Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
    transform.initialize(new MockTransformContext());
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title>" +
          "<author>Giada De Laurentiis</author><year>null</year><price>30.00</price></book>").build(),
      StructuredRecord.builder(INPUT)
        .set("offset", 2)
        .set("body", "<book category=\"children\"><title lang=\"en\">Harry Potter</title>" +
          "<author>J K. Rowling</author><year>2005</year><price>49.99</price></book>").build());

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    for (StructuredRecord record : input) {
      transform.transform(record, emitter);
    }
    Assert.assertEquals(1, emitter.getEmitted().size());
    Assert.assertEquals(0, emitter.getErrors().size());
    StructuredRecord record = emitter.getEmitted().get(0);
    Assert.assertEquals("Harry Potter", record.get("title"));
    Assert.assertEquals("J K. Rowling", record.get("author"));
    Assert.assertEquals(2005, record.get("year"));
  }

  @Test
  public void testExitOnError() throws Exception {
    XMLParser.Config config = new XMLParser.Config("body", "UTF-8", "title:/book/title,author:/book/author," +
      "year:/book/year", "title:string,author:int,year:int", "Exit on error");
    try {
      Transform<StructuredRecord, StructuredRecord> transform = new XMLParser(config);
      transform.initialize(new MockTransformContext());

      StructuredRecord inputRecord = StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "<book category=\"COOKING\"><title lang=\"en\">Everyday Italian</title>" +
          "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book>").build();

      MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
      transform.transform(inputRecord, emitter);
      Assert.fail();
    } catch (IllegalStateException e) {
      LOG.error("Test passes. Exiting on exception.", e);
    }
  }

  @Test
  public void testXpathArray() throws Exception {
    String inputTable = "input-xpath-array";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable));
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put("input", "body")
      .put("encoding", "UTF-8")
      .put("xPathMappings", "category://book/@category,title://book/title")
      .put("fieldTypeMapping", "category:string,title:string")
      .put("processOnError", "Exit on error")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("XMLParser", Transform.PLUGIN_TYPE, sourceProperties, null));
    String sinkTable = "output-xpath-array";

    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("XMLReaderTest");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT)
        .set("offset", 1)
        .set("body", "<bookstore><book category=\"cooking\"><title lang=\"en\">Everyday Italian</title>" +
          "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book>" +
          "<book category=\"children\"><title lang=\"en\">Harry Potter</title><author>J K. Rowling</author>" +
          "<year>2005</year><price>29.99</price></book></bookstore>").build()
    );
    MockSource.writeInput(inputManager, input);
    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    Assert.assertEquals("FAILED", mrManager.getHistory().get(0).getStatus().name());
  }
}
