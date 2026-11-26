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


import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.api.validation.ValidationFailure.Cause;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Unit test for {@link XMLReaderBatchSource.XMLReaderConfig} class.
 */
public class XMLReaderConfigTest {
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
  public void testRejectsUnsafeLegacySecurityProperties() throws Exception {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig(
      "unsafeXmlRef", "/opt/hdfs/catalog.xml", null, "/catalog/book/", "None", null, "Yes",
      "XMLTrackingTable", 30, "/tmp");
    setPluginProperties(config, ImmutableMap.of(
      XMLReaderBatchSource.XMLReaderConfig.ENABLE_EXTERNAL_ENTITIES, "true",
      XMLReaderBatchSource.XMLReaderConfig.SUPPORT_DTD, "${supportDtd}"
    ));

    FailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Set<String> failingProps = failures.stream()
      .map(f -> f.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG))
      .collect(Collectors.toSet());
    Assert.assertTrue(failingProps.contains(XMLReaderBatchSource.XMLReaderConfig.ENABLE_EXTERNAL_ENTITIES));
    Assert.assertTrue(failingProps.contains(XMLReaderBatchSource.XMLReaderConfig.SUPPORT_DTD));
  }

  @Test
  public void testAcceptsSafeLegacySecurityProperties() throws Exception {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig(
      "safeXmlRef", "/opt/hdfs/catalog.xml", null, "/catalog/book/", "None", null, "Yes",
      "XMLTrackingTable", 30, "/tmp");
    setPluginProperties(config, ImmutableMap.of(
      XMLReaderBatchSource.XMLReaderConfig.ENABLE_EXTERNAL_ENTITIES, "false",
      XMLReaderBatchSource.XMLReaderConfig.SUPPORT_DTD, "false"
    ));

    FailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  private static void setPluginProperties(PluginConfig config, Map<String, String> props) throws Exception {
    PluginProperties pluginProperties = PluginProperties.builder().addAll(props).build();
    Field propertiesField = PluginConfig.class.getDeclaredField("properties");
    propertiesField.setAccessible(true);
    propertiesField.set(config, pluginProperties);
    Field rawPropertiesField = PluginConfig.class.getDeclaredField("rawProperties");
    rawPropertiesField.setAccessible(true);
    rawPropertiesField.set(config, pluginProperties);
  }
}
