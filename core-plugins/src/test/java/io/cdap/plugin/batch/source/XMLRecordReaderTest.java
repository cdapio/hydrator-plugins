/*
 * Copyright © 2026 Cask Data, Inc.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLStreamException;

/**
 * Unit tests for {@link XMLRecordReader} XML parsing and XXE hardening.
 */
public class XMLRecordReaderTest {

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testReadsNormalXmlAndBuiltInEntities() throws Exception {
    File xmlFile = tmpFolder.newFile("catalog.xml");
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
      + "<catalog><book><title>Tom &amp; Jerry &lt;1&gt;</title></book></catalog>";
    Files.write(xmlFile.toPath(), xml.getBytes(StandardCharsets.UTF_8));

    List<String> records = readAllRecords(xmlFile, "/catalog/book/title", false);
    Assert.assertEquals(1, records.size());
    Assert.assertEquals("<title>Tom &amp; Jerry &lt;1&gt;</title>", records.get(0));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testBlocksXxeEvenWhenLegacyHadoopConfKeysEnabled() throws Exception {
    File secretFile = tmpFolder.newFile("reader-secret.txt");
    Files.write(secretFile.toPath(), "READER_SECRET_TOKEN".getBytes(StandardCharsets.UTF_8));

    File xmlFile = tmpFolder.newFile("xxe.xml");
    String xxeXml = "<?xml version=\"1.0\"?>"
      + "<!DOCTYPE catalog [<!ENTITY xxe SYSTEM \"" + secretFile.toURI() + "\">]>"
      + "<catalog><book><title>&xxe;</title></book></catalog>";
    Files.write(xmlFile.toPath(), xxeXml.getBytes(StandardCharsets.UTF_8));

    try {
      List<String> records = readAllRecords(xmlFile, "/catalog/book/title", true);
      for (String record : records) {
        Assert.assertFalse("XXE must never leak local file content", record.contains("READER_SECRET_TOKEN"));
      }
      Assert.fail("Expected IllegalArgumentException when parsing XML with DOCTYPE/XXE");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue("Expected XMLStreamException cause, got: " + expected.getCause(),
                        expected.getCause() instanceof XMLStreamException);
      Assert.assertFalse(expected.getMessage() != null && expected.getMessage().contains("READER_SECRET_TOKEN"));
    }
  }

  @Test
  public void testBlocksExternalDtdAndEntityExpansion() throws Exception {
    File xmlFile = tmpFolder.newFile("billion-laughs.xml");
    String dtdXml = "<?xml version=\"1.0\"?>"
      + "<!DOCTYPE catalog [<!ENTITY lol \"lol\"><!ENTITY lol2 \"&lol;&lol;\">]>"
      + "<catalog><book><title>&lol2;</title></book></catalog>";
    Files.write(xmlFile.toPath(), dtdXml.getBytes(StandardCharsets.UTF_8));

    try {
      readAllRecords(xmlFile, "/catalog/book/title", true);
      Assert.fail("Expected IllegalArgumentException when parsing XML with internal DTD entities");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue("Expected XMLStreamException cause, got: " + expected.getCause(),
                        expected.getCause() instanceof XMLStreamException);
    }

    File externalDtdFile = tmpFolder.newFile("external.dtd");
    Files.write(externalDtdFile.toPath(),
                "<!ELEMENT catalog ANY><!ENTITY ext \"EXTERNAL_DTD_CONTENT\">".getBytes(StandardCharsets.UTF_8));
    File extXmlFile = tmpFolder.newFile("external-dtd.xml");
    String extDtdXml = "<?xml version=\"1.0\"?>"
      + "<!DOCTYPE catalog SYSTEM \"" + externalDtdFile.toURI() + "\">"
      + "<catalog><book><title>&ext;</title></book></catalog>";
    Files.write(extXmlFile.toPath(), extDtdXml.getBytes(StandardCharsets.UTF_8));

    try {
      readAllRecords(extXmlFile, "/catalog/book/title", true);
      Assert.fail("Expected IllegalArgumentException when parsing XML with external DTD");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue("Expected XMLStreamException cause, got: " + expected.getCause(),
                        expected.getCause() instanceof XMLStreamException);
      Assert.assertFalse(expected.getMessage() != null && expected.getMessage().contains("EXTERNAL_DTD_CONTENT"));
    }
  }

  @Test
  public void testXIncludeDoesNotResolveExternalFile() throws Exception {
    File secretFile = tmpFolder.newFile("reader-xinclude.txt");
    Files.write(secretFile.toPath(), "READER_XINCLUDE_SECRET".getBytes(StandardCharsets.UTF_8));

    File xmlFile = tmpFolder.newFile("xinclude.xml");
    String xml = "<?xml version=\"1.0\"?>"
      + "<catalog xmlns:xi=\"http://www.w3.org/2001/XInclude\">"
      + "<book><title><xi:include href=\"" + secretFile.toURI() + "\" parse=\"text\"/></title></book>"
      + "</catalog>";
    Files.write(xmlFile.toPath(), xml.getBytes(StandardCharsets.UTF_8));

    List<String> records = readAllRecords(xmlFile, "/catalog/book/title", true);
    Assert.assertEquals(1, records.size());
    for (String record : records) {
      Assert.assertFalse("XInclude must never leak local file content", record.contains("READER_XINCLUDE_SECRET"));
    }
  }

  @SuppressWarnings("deprecation")
  private List<String> readAllRecords(File xmlFile, String nodePath, boolean setLegacyConfFlags) throws Exception {
    Configuration conf = new Configuration();
    conf.set(XMLInputFormat.XML_INPUTFORMAT_NODE_PATH, nodePath);
    conf.set(XMLInputFormat.XML_INPUTFORMAT_FILE_ACTION, "None");
    if (setLegacyConfFlags) {
      conf.setBoolean(XMLInputFormat.XML_INPUTFORMAT_ENABLE_EXTERNAL_ENTITIES, true);
      conf.setBoolean(XMLInputFormat.XML_INPUTFORMAT_SUPPORT_DTD, true);
    }

    Path path = new Path(xmlFile.toURI());
    FileSplit split = new FileSplit(path, 0, xmlFile.length(), new String[0]);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

    List<String> records = new ArrayList<>();
    try (XMLRecordReader reader = new XMLRecordReader()) {
      reader.initialize(split, context);
      while (reader.nextKeyValue()) {
        Map<String, String> value = reader.getCurrentValue();
        records.addAll(value.values());
      }
    }
    return records;
  }
}
