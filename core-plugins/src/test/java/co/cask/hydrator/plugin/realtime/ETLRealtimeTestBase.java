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

package co.cask.hydrator.plugin.realtime;

import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.realtime.ETLRealtimeApplication;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.TestBase;
import co.cask.hydrator.plugin.realtime.sink.RealtimeCubeSink;
import co.cask.hydrator.plugin.realtime.sink.RealtimeKinesisStreamSink;
import co.cask.hydrator.plugin.realtime.sink.RealtimeTableSink;
import co.cask.hydrator.plugin.realtime.sink.StreamSink;
import co.cask.hydrator.plugin.realtime.source.DataGeneratorSource;
import co.cask.hydrator.plugin.realtime.source.JmsSource;
import co.cask.hydrator.plugin.realtime.source.SqsSource;
import co.cask.hydrator.plugin.realtime.source.TwitterSource;
import co.cask.hydrator.plugin.transform.JavaScriptTransform;
import co.cask.hydrator.plugin.transform.ProjectionTransform;
import co.cask.hydrator.plugin.transform.PythonEvaluator;
import co.cask.hydrator.plugin.transform.ScriptFilterTransform;
import co.cask.hydrator.plugin.transform.StructuredRecordToGenericRecordTransform;
import co.cask.hydrator.plugin.transform.ValidatorTransform;
import org.junit.BeforeClass;
import org.python.util.PythonInterpreter;

/**
 * Performs common setup logic
 */
public class ETLRealtimeTestBase extends TestBase {

  protected static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("etlrealtime", "3.2.0");
  protected static final ArtifactSummary APP_ARTIFACT =
    new ArtifactSummary(APP_ARTIFACT_ID.getArtifact(), APP_ARTIFACT_ID.getVersion());

  @BeforeClass
  public static void setupTests() throws Exception {

    addAppArtifact(APP_ARTIFACT_ID, ETLRealtimeApplication.class,
                   RealtimeSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    addPluginArtifact(NamespaceId.DEFAULT.artifact("core-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      RealtimeKinesisStreamSink.class,
                      DataGeneratorSource.class, JmsSource.class,
                      TwitterSource.class, SqsSource.class,
                      RealtimeCubeSink.class, RealtimeTableSink.class,
                      StreamSink.class,
                      ProjectionTransform.class, ScriptFilterTransform.class,
                      JavaScriptTransform.class, ValidatorTransform.class,
                      PythonEvaluator.class, PythonInterpreter.class,
                      StructuredRecordToGenericRecordTransform.class);
  }
}
