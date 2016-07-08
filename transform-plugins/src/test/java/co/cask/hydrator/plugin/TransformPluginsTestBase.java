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

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.etl.batch.ETLBatchApplication;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.TestConfiguration;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.csv.CSVFormat;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * Test base for Transform Plugins.
 */
public class TransformPluginsTestBase extends HydratorTestBase {

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.2.0");

  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("etlbatch", CURRENT_VERSION.getVersion());
  protected static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // Add the ETL batch artifact and mock plugins.
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, ETLBatchApplication.class);

    // Add our plugins artifact with the ETL batch artifact as its parent.
    // This will make our plugins available to the ETL batch.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("transform-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      ValueMapper.class, CSVFormat.class, Base64.class, JSONParser.class, XMLToJSON.class);
  }
}
