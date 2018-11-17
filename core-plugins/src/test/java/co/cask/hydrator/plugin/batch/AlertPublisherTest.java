/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.etl.api.Alert;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.mock.alert.NullAlertTransform;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.proto.ArtifactSelectorConfig;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests AlertPublisher plugins
 */
public class AlertPublisherTest extends ETLBatchTestBase {
  private static final Gson GSON = new Gson();

  @Ignore
  @Test
  public void testAlertPublisher() throws Exception {
    String sourceName = "alertSource";
    String sinkName = "alertSink";
    String topic = "alertTopic";

    // need to specify this, otherwise the mock TMS plugin will be used.
    ArtifactSelectorConfig corePluginsArtifact = new ArtifactSelectorConfig("USER", "core-plugins", "1.0.0");

    /*
     * source --> nullAlert --> sink
     *               |
     *               |--> TMS publisher
     */
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName)))
      .addStage(new ETLStage("nullAlert", NullAlertTransform.getPlugin("id")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addStage(new ETLStage("tms", new ETLPlugin("TMS", AlertPublisher.PLUGIN_TYPE,
                                                  ImmutableMap.of("topic", topic, "autoCreateTopic", "true"),
                                                  corePluginsArtifact)))
      .addConnection("source", "nullAlert")
      .addConnection("nullAlert", "sink")
      .addConnection("nullAlert", "tms")
      .build();

    ApplicationManager appManager = deployETL(config, "AlertTest");

    Schema schema = Schema.recordOf("x", Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
    StructuredRecord record1 = StructuredRecord.builder(schema).set("id", 1L).build();
    StructuredRecord record2 = StructuredRecord.builder(schema).set("id", 2L).build();
    StructuredRecord alertRecord = StructuredRecord.builder(schema).build();

    DataSetManager<Table> sourceTable = getDataset(sourceName);
    MockSource.writeInput(sourceTable, ImmutableList.of(record1, record2, alertRecord));

    runETLOnce(appManager);

    DataSetManager<Table> sinkTable = getDataset(sinkName);
    Set<StructuredRecord> actual = new HashSet<>(MockSink.readOutput(sinkTable));
    Set<StructuredRecord> expected = ImmutableSet.of(record1, record2);
    Assert.assertEquals(expected, actual);

    MessageFetcher messageFetcher = getMessagingContext().getMessageFetcher();
    Set<Alert> actualMessages = new HashSet<>();
    try (CloseableIterator<Message> iter = messageFetcher.fetch(NamespaceId.DEFAULT.getNamespace(), topic, 5, 0)) {
      while (iter.hasNext()) {
        Message message = iter.next();
        Alert alert = GSON.fromJson(message.getPayloadAsString(), Alert.class);
        actualMessages.add(alert);
      }
    }
    Set<Alert> expectedMessages = ImmutableSet.of(new Alert("nullAlert", new HashMap<String, String>()));
    Assert.assertEquals(expectedMessages, actualMessages);
  }
}
