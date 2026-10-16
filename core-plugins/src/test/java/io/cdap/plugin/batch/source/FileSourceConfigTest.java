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

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * Tests that reserved Hadoop keys in the File source's file system properties are rejected and never reach the
 * Hadoop configuration.
 */
public class FileSourceConfigTest {

  private static final String RESERVED_PROPERTIES =
    "{\"fs.gs.storage.root.url\":\"http://127.0.0.1:1/\","
      + "\"fs.gs.token.server.url\":\"http://127.0.0.1:1/tok\","
      + "\"fs.gs.project.id\":\"x\"}";
  private static final String ALLOWED_PROPERTIES =
    "{\"fs.gs.inputstream.fadvise\":\"AUTO\",\"fs.gs.project.id\":\"x\"}";

  @Test
  public void testReservedPropertiesFailValidation() throws Exception {
    FileSourceConfig config = createConfig("gs://bucket/x", "csv", RESERVED_PROPERTIES);
    MockFailureCollector collector = new MockFailureCollector("source");
    config.validate(collector);

    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    for (ValidationFailure failure : failures) {
      Assert.assertEquals(FileSourceConfig.NAME_FILE_SYSTEM_PROPERTIES,
                          failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testFallbackPrefixAndImpersonationFailValidation() throws Exception {
    String properties = "{\"google.cloud.proxy.address\":\"127.0.0.1:1\","
      + "\"fs.gs.auth.impersonation.service.account\":\"other@project.iam.gserviceaccount.com\","
      + "\"cdap.auth.service.account\":\"{}\"}";
    FileSourceConfig config = createConfig("gs://bucket/x", "csv", properties);
    MockFailureCollector collector = new MockFailureCollector("source");
    config.validate(collector);
    Assert.assertEquals(3, collector.getValidationFailures().size());
  }

  @Test
  public void testAllowedPropertiesPassValidation() throws Exception {
    FileSourceConfig config = createConfig("gs://bucket/dir/", "csv", ALLOWED_PROPERTIES);
    MockFailureCollector collector = new MockFailureCollector("source");
    config.validate(collector);
    Assert.assertTrue(collector.getValidationFailures().toString(), collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidJsonFailsValidation() throws Exception {
    FileSourceConfig config = createConfig("gs://bucket/dir/", "csv", "{not json");
    MockFailureCollector collector = new MockFailureCollector("source");
    try {
      config.validate(collector);
    } catch (RuntimeException e) {
      // ValidationFailure.withStacktrace() is not usable on JDK 17+ in unit tests; the failure is still recorded
    }
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testReservedPropertiesNeverReachTheHadoopConfiguration() throws Exception {
    // covers values that are only known after validation, e.g. when the property is a macro
    FileBatchSource source = new FileBatchSource(createConfig("gs://bucket/x", "csv", RESERVED_PROPERTIES));
    Map<String, String> properties = source.getFileSystemProperties(null);
    Assert.assertFalse(properties.containsKey("fs.gs.storage.root.url"));
    Assert.assertFalse(properties.containsKey("fs.gs.token.server.url"));
    Assert.assertEquals("x", properties.get("fs.gs.project.id"));
  }

  @Test
  public void testAllowedPropertiesReachTheHadoopConfiguration() throws Exception {
    FileBatchSource source = new FileBatchSource(createConfig("gs://bucket/dir/", "csv", ALLOWED_PROPERTIES));
    Map<String, String> properties = source.getFileSystemProperties(null);
    Assert.assertEquals("AUTO", properties.get("fs.gs.inputstream.fadvise"));
    Assert.assertEquals("x", properties.get("fs.gs.project.id"));
  }

  private static FileSourceConfig createConfig(String path, String format, String fileSystemProperties)
    throws Exception {
    FileSourceConfig config = new FileSourceConfig();
    setField(AbstractFileSourceConfig.class, config, "referenceName", "ref");
    setField(AbstractFileSourceConfig.class, config, "format", format);
    setField(FileSourceConfig.class, config, "path", path);
    setField(FileSourceConfig.class, config, "fileSystemProperties", fileSystemProperties);
    return config;
  }

  private static void setField(Class<?> clz, Object target, String name, Object value) throws Exception {
    Field field = clz.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
