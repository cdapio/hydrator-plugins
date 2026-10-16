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

package io.cdap.plugin.batch.sink;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.format.plugin.AbstractFileSinkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * Tests that reserved Hadoop keys in the File sink's file system properties are rejected and never reach the
 * Hadoop configuration.
 */
public class FileSinkConfigTest {

  private static final String RESERVED_PROPERTIES =
    "{\"fs.gs.storage.root.url\":\"http://127.0.0.1:1/\",\"google.cloud.auth.access.token.provider.impl\":\"x\","
      + "\"fs.gs.outputstream.type\":\"BASIC\"}";

  @Test
  public void testReservedPropertiesFailValidation() throws Exception {
    FileSink.Conf config = createConfig("gs://bucket/out/", RESERVED_PROPERTIES);
    MockFailureCollector collector = new MockFailureCollector("sink");
    config.validate(collector);

    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    for (ValidationFailure failure : failures) {
      Assert.assertEquals("fileSystemProperties",
                          failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testReservedPropertiesNeverReachTheHadoopConfiguration() throws Exception {
    FileSink sink = new FileSink(createConfig("gs://bucket/out/", RESERVED_PROPERTIES));
    Map<String, String> properties = sink.getFileSystemProperties(null);
    Assert.assertFalse(properties.containsKey("fs.gs.storage.root.url"));
    Assert.assertFalse(properties.containsKey("google.cloud.auth.access.token.provider.impl"));
    Assert.assertEquals("BASIC", properties.get("fs.gs.outputstream.type"));
  }

  private static FileSink.Conf createConfig(String path, String fileSystemProperties) throws Exception {
    Constructor<FileSink.Conf> constructor = FileSink.Conf.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    FileSink.Conf config = constructor.newInstance();
    setField(AbstractFileSinkConfig.class, config, "referenceName", "ref");
    setField(AbstractFileSinkConfig.class, config, "format", "csv");
    setField(FileSink.Conf.class, config, "path", path);
    setField(FileSink.Conf.class, config, "fileSystemProperties", fileSystemProperties);
    return config;
  }

  private static void setField(Class<?> clz, Object target, String name, Object value) throws Exception {
    Field field = clz.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
