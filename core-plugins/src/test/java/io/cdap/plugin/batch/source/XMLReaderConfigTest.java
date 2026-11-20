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
package io.cdap.plugin.batch.source;


import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure.Cause;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import javax.xml.stream.XMLStreamException;

/**
 * Unit test for {@link XMLReaderBatchSource.XMLReaderConfig} class.
 */
public class XMLReaderConfigTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  @Test
  public void testValidateConfig() {
    String path = "/opt/hdfs/catalog.xml";
    String nodePath = "/catalog/book/";
    String reprocessingRequired = "Yes";
    String tableName = "XMLTrackingTable";
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("validReference", path,
                                                                                           null, nodePath, null, null,
                                                                                           reprocessingRequired,
                                                                                           tableName, 30, "/tmp");
    Assert.assertEquals(path, config.getPath());
    Assert.assertEquals(nodePath, config.getNodePath());
    Assert.assertEquals(true, config.isReprocessingRequired());
    Assert.assertEquals(tableName, config.getTableName());
  }

  @Test
  public void testEmptyPath() {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("emptyPathReference", "",
                                                                                           null, "/catalog/book/",
                                                                                           null, null, "Yes",
                                                                                           "XMLTrackingTable", 30,
                                                                                           "/tmp");
    FailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(1, collector.getValidationFailures().get(0).getCauses().size());
    Cause expectedCause = new Cause();
    expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, XMLReaderBatchSource.XMLReaderConfig.PATH);
    Assert.assertEquals(expectedCause, collector.getValidationFailures().get(0).getCauses().get(0));
  }

  @Test
  public void testEmptyNodePath() {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("emptyNodePathReference",
                                                                                           "/opt/hdfs/catalog.xml",
                                                                                           null, "", null, null,
                                                                                           "Yes", "XMLTrackingTable",
                                                                                           30, "/tmp");
    FailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(1, collector.getValidationFailures().get(0).getCauses().size());
    Cause expectedCause = new Cause();
    expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, XMLReaderBatchSource.XMLReaderConfig.NODE_PATH);
    Assert.assertEquals(expectedCause, collector.getValidationFailures().get(0).getCauses().get(0));
  }

  @Test
  public void testActionAfterProcessAndReprocessingConflict() {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("conflictReference",
                                                                                           "/opt/hdfs/catalog.xml",
                                                                                           null, "/catalog/book/",
                                                                                           "Delete", null, "Yes",
                                                                                           "XMLTrackingTable", 30,
                                                                                           "/tmp");
    FailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(2, collector.getValidationFailures().get(0).getCauses().size());
  }

  @Test
  public void testEmptyTargetFolder() {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("emptyTargetFolderReference",
                                                                                           "/opt/hdfs/catalog.xml",
                                                                                           null, "/catalog/book/",
                                                                                           "Move", "", "No",
                                                                                           "XMLTrackingTable", 30,
                                                                                           "/tmp");
    FailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(1, collector.getValidationFailures().get(0).getCauses().size());
    Cause expectedCause = new Cause();
    expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, XMLReaderBatchSource.XMLReaderConfig.TARGET_FOLDER);
    Assert.assertEquals(expectedCause, collector.getValidationFailures().get(0).getCauses().get(0));
  }

  @Test
  public void testEmptyTemporaryFolder() {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("emptyNodePathReference",
                                                                                           "/opt/hdfs/catalog.xml",
                                                                                           null, "/catalog/book/",
                                                                                           "Delete", null, "No",
                                                                                           "XMLTrackingTable", 30,
                                                                                           null);
    FailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(1, collector.getValidationFailures().get(0).getCauses().size());
    Cause expectedCause = new Cause();
    expectedCause.addAttribute(CauseAttributes.STAGE_CONFIG, XMLReaderBatchSource.XMLReaderConfig.TEMPORARY_FOLDER);
    Assert.assertEquals(expectedCause, collector.getValidationFailures().get(0).getCauses().get(0));
  }

  @Test
  public void testExternalEntitiesAndDtdAlwaysDisabledInConfigAndRecordReader() throws Exception {
    File secretFile = TEMP_FOLDER.newFile("xmlreader-secret.txt");
    Files.write(secretFile.toPath(), "SECRET_TOKEN_VALUE".getBytes(StandardCharsets.UTF_8));
    File dtdFile = TEMP_FOLDER.newFile("xmlreader-external.dtd");
    Files.write(dtdFile.toPath(), "<!ENTITY extParam \"LEAKED\">".getBytes(StandardCharsets.UTF_8));

    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig(
      "xxeReference", "/opt/hdfs/catalog.xml", null, "/catalog/book", "None", null, "Yes",
      null, null, "/tmp", true, true);
    Assert.assertFalse(config.shouldEnableExternalEntities());
    Assert.assertFalse(config.shouldSupportDTD());

    String[] disallowedPayloads = new String[] {
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<!DOCTYPE catalog [\n"
        + "  <!ENTITY xxe SYSTEM \"" + secretFile.toURI() + "\">\n"
        + "]>\n"
        + "<catalog><book><title>&xxe;</title></book></catalog>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<!DOCTYPE catalog SYSTEM \"" + dtdFile.toURI() + "\">\n"
        + "<catalog><book><title>Test</title></book></catalog>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<!DOCTYPE catalog [\n"
        + "  <!ENTITY % remote SYSTEM \"" + dtdFile.toURI() + "\">\n"
        + "  %remote;\n"
        + "]>\n"
        + "<catalog><book><title>Test</title></book></catalog>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<catalog>\n"
        + "  <book><title><xi:include xmlns:xi=\"http://www.w3.org/2001/XInclude\" href=\""
        + secretFile.toURI() + "\" parse=\"text\"/></title></book>\n"
        + "</catalog>",
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<catalog>\n"
        + "  <book><title><include xmlns=\"http://www.w3.org/2001/XInclude\" href=\""
        + secretFile.toURI() + "\" parse=\"text\"/></title></book>\n"
        + "</catalog>"
    };

    for (int i = 0; i < disallowedPayloads.length; i++) {
      File xmlFile = TEMP_FOLDER.newFile("disallowed-" + i + ".xml");
      Files.write(xmlFile.toPath(), disallowedPayloads[i].getBytes(StandardCharsets.UTF_8));
      Configuration hConf = new Configuration();
      hConf.set(XMLInputFormat.XML_INPUTFORMAT_NODE_PATH, "/catalog/book");
      hConf.set(XMLInputFormat.XML_INPUTFORMAT_FILE_ACTION, "None");
      hConf.setBoolean(XMLInputFormat.XML_INPUTFORMAT_ENABLE_EXTERNAL_ENTITIES, true);
      hConf.setBoolean(XMLInputFormat.XML_INPUTFORMAT_SUPPORT_DTD, true);

      FileSplit split = new FileSplit(new Path(xmlFile.toURI()), 0, xmlFile.length(), null);
      TaskAttemptContext context = new TaskAttemptContextImpl(hConf, new TaskAttemptID());
      try (XMLRecordReader recordReader = new XMLRecordReader()) {
        recordReader.initialize(split, context);
        try {
          while (recordReader.nextKeyValue()) {
            Assert.fail("Expected exception for disallowed XML payload in " + xmlFile.getName());
          }
          Assert.fail("Expected exception for disallowed XML payload in " + xmlFile.getName());
        } catch (IllegalArgumentException expected) {
          Assert.assertTrue(expected.getCause() instanceof XMLStreamException);
          Assert.assertFalse(expected.getMessage().contains("SECRET_TOKEN_VALUE"));
          Assert.assertFalse(expected.getMessage().contains("LEAKED"));
        }
      }
    }

    // Verify valid XML with predefined entities still parses cleanly.
    File validFile = TEMP_FOLDER.newFile("valid-catalog.xml");
    String validXml =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<catalog><book><title>A &amp; B &lt;C&gt;</title></book></catalog>";
    Files.write(validFile.toPath(), validXml.getBytes(StandardCharsets.UTF_8));
    Configuration hConf = new Configuration();
    hConf.set(XMLInputFormat.XML_INPUTFORMAT_NODE_PATH, "/catalog/book");
    hConf.set(XMLInputFormat.XML_INPUTFORMAT_FILE_ACTION, "None");
    FileSplit split = new FileSplit(new Path(validFile.toURI()), 0, validFile.length(), null);
    TaskAttemptContext context = new TaskAttemptContextImpl(hConf, new TaskAttemptID());
    try (XMLRecordReader recordReader = new XMLRecordReader()) {
      recordReader.initialize(split, context);
      Assert.assertTrue(recordReader.nextKeyValue());
      String record = recordReader.getCurrentValue().values().iterator().next();
      Assert.assertEquals("<book><title>A &amp; B &lt;C&gt;</title></book>", record);
      Assert.assertFalse(recordReader.nextKeyValue());
    }
  }
}
