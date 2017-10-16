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

package co.cask.hydrator.plugin.batch.action;

import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.test.ApplicationManager;
import co.cask.hydrator.plugin.batch.ETLBatchTestBase;
import com.dumbster.smtp.SimpleSmtpServer;
import com.dumbster.smtp.SmtpMessage;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;

/**
 * Test for {@link EmailAction}
 */
public class EmailActionTestRun extends ETLBatchTestBase {

  private SimpleSmtpServer server;
  private int port;

  // TODO: CDAP-12368 Uncomment the @Before method while fixing CDAP-12368. Commenting out because the setup causes OOM
  /*@Before
  public void beforeTest() {
    port = Networks.getRandomPort();
    server = SimpleSmtpServer.start(port);
  }*/

  @Ignore
  @Test
  public void testEmailAction() throws Exception {

    ETLStage action = new ETLStage(
      "email",
      new ETLPlugin("Email", PostAction.PLUGIN_TYPE,
                    ImmutableMap.of("recipients", "to@test.com",
                                    "sender", "from@test.com",
                                    "message", "Run for ${logicalStartTime(yyyy-MM-dd,0m,UTC)} completed.",
                                    "subject", "Test",
                                    "port", Integer.toString(port)),
                    null));

    ETLStage source = new ETLStage("source",
                                   new ETLPlugin("KVTable", BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of("name", "emailTestSource"), null));
    ETLStage sink = new ETLStage("sink", new ETLPlugin("KVTable", BatchSink.PLUGIN_TYPE,
                                                       ImmutableMap.of("name", "emailTestSink"), null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addPostAction(action)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationManager appManager = deployETL(etlConfig, "actionTest");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", "0"));

    server.stop();

    Assert.assertEquals(1, server.getReceivedEmailSize());
    Iterator emailIter = server.getReceivedEmail();
    SmtpMessage email = (SmtpMessage) emailIter.next();
    Assert.assertEquals("Test", email.getHeaderValue("Subject"));
    Assert.assertTrue(email.getBody().startsWith("Run for 1970-01-01 completed."));
    Assert.assertFalse(emailIter.hasNext());
  }
}
