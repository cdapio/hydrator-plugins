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

package co.cask.hydrator.common;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.batch.BatchContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class MacroConfigTest {
  private static final String CONSTANT = "${runtime:yyyy}";

  @Test
  public void testNoOp() {
    String value = "abc123";
    TestConfig testConfig = new TestConfig(value);
    testConfig.substituteMacros(new MockBatchContext(0));
    Assert.assertEquals(value, testConfig.stringField);
    Assert.assertEquals(CONSTANT, TestConfig.CONSTANT);
  }

  @Test
  public void testSubstitution() {
    BatchContext mockContext = new MockBatchContext(0);

    TestConfig testConfig = new TestConfig("${runtime:yyyy-MM-dd'T'HH:mm:ss}", "0m", "UTC");
    testConfig.substituteMacros(mockContext);
    Assert.assertEquals("1970-01-01T00:00:00", testConfig.stringField);

    testConfig = new TestConfig("${runtime:yyyy}${runtime:MM}", "0m", "UTC");
    testConfig.substituteMacros(mockContext);
    Assert.assertEquals("197001", testConfig.stringField);

    testConfig = new TestConfig("abc-${runtime:yyyy}-123", "0m", "UTC");
    testConfig.substituteMacros(mockContext);
    Assert.assertEquals("abc-1970-123", testConfig.stringField);

    testConfig = new TestConfig("$${{}}{${runtime:yyyy}}}{}$$", "0m", "UTC");
    testConfig.substituteMacros(mockContext);
    Assert.assertEquals("$${{}}{1970}}{}$$", testConfig.stringField);
  }

  @Test
  public void testUnenclosedMacro() {
    BatchContext mockContext = new MockBatchContext(0);

    TestConfig testConfig = new TestConfig("${runtime:yyyy-MM-dd'T'HH:mm:ss", "0m", "UTC");
    testConfig.substituteMacros(mockContext);
    Assert.assertEquals("${runtime:yyyy-MM-dd'T'HH:mm:ss", testConfig.stringField);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPattern() {
    BatchContext mockContext = new MockBatchContext(0);

    TestConfig testConfig = new TestConfig("${runtime:asdf}", "0m", "UTC");
    testConfig.substituteMacros(mockContext);
  }

  // unused fields are ok, they are there just to make sure we don't choke on them while reflecting.
  @SuppressWarnings("unused")
  private static class TestConfig extends MacroConfig {
    // shouldn't get substituted
    private static final String CONSTANT = MacroConfigTest.CONSTANT;

    // shouldn't break substitution
    private Integer intField;
    private Long longField;
    private Double doubleField;
    private Float floatField;
    private Boolean boolField;

    // should get substituted
    private String stringField;

    public TestConfig(String stringField) {
      super();
      this.stringField = stringField;
    }

    public TestConfig(String stringField, String offset, String timeZone) {
      super(offset, timeZone);
      this.stringField = stringField;
    }
  }

  private static class MockBatchContext implements BatchContext {
    private long runtime;

    public MockBatchContext(long runtime) {
      this.runtime = runtime;
    }

    @Override
    public long getLogicalStartTime() {
      return runtime;
    }

    @Override
    public Map<String, String> getRuntimeArguments() {
      return new HashMap<>();
    }

    @Override
    public void setRuntimeArgument(String s, String s1, boolean b) {

    }

    @Override
    public <T> T getHadoopJob() {
      return null;
    }

    @Override
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
      return null;
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments) throws
      DatasetInstantiationException {
      return null;
    }

    @Override
    public void releaseDataset(Dataset dataset) {

    }

    @Override
    public void discardDataset(Dataset dataset) {

    }

    @Override
    public PluginProperties getPluginProperties() {
      return null;
    }

    @Override
    public StageMetrics getMetrics() {
      return null;
    }

    @Override
    public String getStageName() {
      return null;
    }

    @Override
    public <T> Lookup<T> provide(String s, Map<String, String> map) {
      return null;
    }

    @Override
    public PluginProperties getPluginProperties(String pluginId) {
      return null;
    }

    @Override
    public <T> Class<T> loadPluginClass(String pluginId) {
      return null;
    }

    @Override
    public <T> T newPluginInstance(String pluginId) throws InstantiationException {
      return null;
    }
  }
}
