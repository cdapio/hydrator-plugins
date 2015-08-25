/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.plugin.etl.pluginsTest;

import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.api.PipelineConfigurable;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.batch.ETLBatchTemplate;
import co.cask.cdap.template.etl.common.ETLConfig;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.plugin.etl.sink.BatchCassandraSink;
import co.cask.plugin.etl.sink.TableSink;
import co.cask.plugin.etl.source.CassandraBatchSource;
import com.google.gson.Gson;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import co.cask.plugin.etl.source.StreamBatchSource;

import java.io.IOException;

/**
 * Base test class that sets up plugins and the batch template.
 */
public class BaseETLBatchTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final Id.Namespace NAMESPACE = Id.Namespace.DEFAULT;
  protected static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("ETLBatch");

  @BeforeClass
  public static void setupTest() throws IOException {
    addTemplatePlugins(TEMPLATE_ID, "batch-plugins-1.0.0.jar",
                       StreamBatchSource.class, CassandraBatchSource.class, TableSink.class, BatchCassandraSink.class);
    deployTemplate(NAMESPACE, TEMPLATE_ID, ETLBatchTemplate.class,
                   PipelineConfigurable.class.getPackage().getName(),
                   BatchSource.class.getPackage().getName(),
                   ETLConfig.class.getPackage().getName());
  }
}
