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

package io.cdap.plugin.format;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link FileSystemPropertiesValidator}.
 */
public class FileSystemPropertiesValidatorTest {

  private static final List<String> RESERVED = Arrays.asList(
    "fs.gs.storage.root.url",
    "fs.gs.storage.service.path",
    "fs.gs.token.server.url",
    "fs.gs.http.transport.type",
    "fs.gs.enable.service.account.auth",
    "fs.gs.service.account.auth.email",
    "fs.gs.service.account.auth.keyfile",
    "fs.gs.impl",
    "fs.AbstractFileSystem.gs.impl",
    "fs.gs.proxy.address",
    "fs.gs.proxy.username",
    "fs.gs.proxy.password",
    "fs.gs.auth.access.token.provider.impl",
    "fs.gs.auth.impersonation.service.account",
    "fs.gs.auth.impersonation.service.account.for.user.alice",
    "fs.gs.auth.service.account.json.keyfile",
    "fs.gs.auth.service.account.private.key",
    "fs.gs.auth.client.secret",
    "fs.gs.auth.refresh.token",
    "fs.gs.auth.null.enable",
    "fs.gs.authorization.handler.impl",
    "fs.gs.authorization.handler.properties.foo",
    "fs.gs.delegation.token.binding",
    "google.cloud.token.server.url",
    "google.cloud.proxy.address",
    "google.cloud.auth.service.account.json.keyfile",
    "google.cloud.auth.access.token.provider.impl",
    "cdap.auth.service.account",
    "cdap.auth.service.account.type",
    "cdap.auth.service.account.scopes",
    " fs.gs.storage.root.url ");

  private static final List<String> ALLOWED = Arrays.asList(
    "fs.gs.inputstream.fadvise",
    "fs.gs.outputstream.type",
    "fs.gs.block.size",
    "fs.gs.status.parallel.enable",
    "fs.gs.list.max.items.per.call",
    "fs.gs.requester.pays.mode",
    "fs.gs.requester.pays.project.id",
    "fs.gs.project.id",
    "fs.gs.system.bucket",
    "fs.gs.impl.disable.cache",
    "fs.s3a.access.key",
    "fs.s3a.endpoint",
    "fs.azure.account.key.myaccount.blob.core.windows.net",
    "mapreduce.input.fileinputformat.split.minsize",
    "io.file.buffer.size",
    "textinputformat.record.delimiter");

  @Test
  public void testReservedKeys() {
    for (String key : RESERVED) {
      Assert.assertTrue("expected key to be reserved: " + key,
                        FileSystemPropertiesValidator.isReservedFileSystemProperty(key));
    }
  }

  @Test
  public void testAllowedKeys() {
    for (String key : ALLOWED) {
      Assert.assertFalse("expected key to be allowed: " + key,
                         FileSystemPropertiesValidator.isReservedFileSystemProperty(key));
    }
    Assert.assertFalse(FileSystemPropertiesValidator.isReservedFileSystemProperty(null));
  }

  @Test
  public void testValidateReportsEveryReservedKeyAgainstProperty() {
    MockFailureCollector collector = new MockFailureCollector("stage");
    Map<String, String> properties = ImmutableMap.of(
      "fs.gs.storage.root.url", "http://127.0.0.1:1/",
      "fs.gs.token.server.url", "http://127.0.0.1:1/tok",
      "fs.gs.project.id", "x");
    FileSystemPropertiesValidator.validateFileSystemProperties(properties, "fileSystemProperties", collector);

    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    for (ValidationFailure failure : failures) {
      Assert.assertEquals(1, failure.getCauses().size());
      Assert.assertEquals("fileSystemProperties",
                          failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateAcceptsAllowedKeysAndNull() {
    MockFailureCollector collector = new MockFailureCollector("stage");
    FileSystemPropertiesValidator.validateFileSystemProperties(
      ImmutableMap.of("fs.gs.inputstream.fadvise", "AUTO"), "fileSystemProperties", collector);
    FileSystemPropertiesValidator.validateFileSystemProperties(null, "fileSystemProperties", collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testRemoveReservedKeys() {
    Map<String, String> properties = new HashMap<>();
    properties.put("fs.gs.storage.root.url", "http://127.0.0.1:1/");
    properties.put("google.cloud.proxy.address", "127.0.0.1:1");
    properties.put("cdap.auth.service.account", "{}");
    properties.put("fs.gs.inputstream.fadvise", "AUTO");

    Map<String, String> filtered = FileSystemPropertiesValidator.removeReservedFileSystemProperties(properties);
    Assert.assertEquals(ImmutableMap.of("fs.gs.inputstream.fadvise", "AUTO"), filtered);
    // the input map is left untouched
    Assert.assertEquals(4, properties.size());
    // immutable and null inputs are accepted
    Assert.assertEquals(ImmutableMap.of("fs.gs.block.size", "1"),
                        FileSystemPropertiesValidator.removeReservedFileSystemProperties(
                          ImmutableMap.of("fs.gs.block.size", "1", "fs.gs.token.server.url", "http://127.0.0.1:1")));
    Assert.assertTrue(FileSystemPropertiesValidator.removeReservedFileSystemProperties(null).isEmpty());
  }
}
