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
package co.cask.hydrator.plugin.batch.source;


import org.junit.Assert;
import org.junit.Test;

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

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyPath() {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("emptyPathReference", "",
                                                                                           null, "/catalog/book/",
                                                                                           null, null, "Yes",
                                                                                           "XMLTrackingTable", 30,
                                                                                           "/tmp");
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyNodePath() {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("emptyNodePathReference",
                                                                                           "/opt/hdfs/catalog.xml",
                                                                                           null, "", null, null,
                                                                                           "Yes", "XMLTrackingTable",
                                                                                           30, "/tmp");
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testActionAfterProcessAndReprocessingConflict() {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("conflictReference",
                                                                                           "/opt/hdfs/catalog.xml",
                                                                                           null, "/catalog/book/",
                                                                                           "Delete", null, "Yes",
                                                                                           "XMLTrackingTable", 30,
                                                                                           "/tmp");
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyTargetFolder() {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("emptyTargetFolderReference",
                                                                                           "/opt/hdfs/catalog.xml",
                                                                                           null, "/catalog/book/",
                                                                                           "Move", "", "No",
                                                                                           "XMLTrackingTable", 30,
                                                                                           "/tmp");
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyTemporaryFolder() {
    XMLReaderBatchSource.XMLReaderConfig config = new XMLReaderBatchSource.XMLReaderConfig("emptyNodePathReference",
                                                                                           "/opt/hdfs/catalog.xml",
                                                                                           null, "/catalog/book/",
                                                                                           "Delete", null, "No",
                                                                                           "XMLTrackingTable", 30,
                                                                                           null);
    config.validate();
  }
}
