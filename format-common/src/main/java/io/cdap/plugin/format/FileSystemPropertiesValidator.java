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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Checks user supplied Hadoop file system properties for keys that must not be overridden.
 *
 * <p>File based plugins copy the 'fileSystemProperties' map into the Hadoop configuration that is used to open the
 * file system. The Google Cloud Storage connector reads the storage endpoint, token server, proxy, credential
 * sources, impersonation target and several implementation classes from that configuration. Letting a user
 * override them would allow the credentials of the environment the plugin runs in (for example, the service account
 * of the CDAP system service during pipeline validation) to be sent to an arbitrary endpoint or used to impersonate
 * another identity.
 */
public final class FileSystemPropertiesValidator {

  /**
   * Hadoop configuration keys that users are not allowed to set through the 'fileSystemProperties' plugin property.
   */
  private static final Set<String> RESERVED_FS_PROPERTY_KEYS = ImmutableSet.of(
    // GCS API endpoint. The connector sends 'Authorization: Bearer <token>' to whatever is configured here.
    "fs.gs.storage.root.url",
    "fs.gs.storage.service.path",
    // OAuth token endpoint used to obtain or refresh access tokens.
    "fs.gs.token.server.url",
    // HTTP transport used by the credential and proxy code paths.
    "fs.gs.http.transport.type",
    // Deprecated aliases of 'fs.gs.auth.service.account.*' keys that the connector still honors.
    "fs.gs.enable.service.account.auth",
    "fs.gs.service.account.auth.email",
    "fs.gs.service.account.auth.keyfile",
    // FileSystem implementation classes.
    "fs.gs.impl",
    "fs.AbstractFileSystem.gs.impl");

  /**
   * Prefixes of Hadoop configuration keys that users are not allowed to set through the 'fileSystemProperties'
   * plugin property. See {@link #RESERVED_FS_PROPERTY_KEYS}.
   */
  private static final List<String> RESERVED_FS_PROPERTY_PREFIXES = ImmutableList.of(
    // Proxy address, username and password. Routes every connector request through a user-controlled host.
    "fs.gs.proxy.",
    // Credential sources, access token provider class and service account impersonation.
    "fs.gs.auth.",
    // Classes instantiated by the connector.
    "fs.gs.authorization.handler.",
    "fs.gs.delegation.token.",
    // The connector resolves every 'fs.gs.<suffix>' credential key through 'google.cloud.<suffix>' as well,
    // so the fallback prefix has to be reserved too or the keys above could be set through it.
    "google.cloud.",
    // Keys used by the Google Cloud plugins to pass their service account to the connector's token provider.
    "cdap.auth.");

  private FileSystemPropertiesValidator() {
    // no-op
  }

  /**
   * Returns whether the given Hadoop configuration key is reserved and must not be set by users through the
   * 'fileSystemProperties' plugin property.
   *
   * @param key the Hadoop configuration key
   * @return true if the key is reserved
   */
  public static boolean isReservedFileSystemProperty(@Nullable String key) {
    if (key == null) {
      return false;
    }
    String trimmedKey = key.trim();
    if (RESERVED_FS_PROPERTY_KEYS.contains(trimmedKey)) {
      return true;
    }
    for (String prefix : RESERVED_FS_PROPERTY_PREFIXES) {
      if (trimmedKey.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Validates that user supplied file system properties do not set any reserved Hadoop configuration key.
   * One failure is added to the collector for every reserved key found, so that all offending keys are reported
   * at once.
   *
   * @param properties the user supplied file system properties, may be null
   * @param configProperty the name of the plugin property the file system properties were read from
   * @param collector the failure collector to report reserved keys to
   */
  public static void validateFileSystemProperties(@Nullable Map<String, String> properties, String configProperty,
                                                  FailureCollector collector) {
    if (properties == null) {
      return;
    }
    for (String key : properties.keySet()) {
      if (isReservedFileSystemProperty(key)) {
        collector.addFailure(
          String.format("File system property '%s' is reserved and cannot be overridden.", key),
          "Remove it from the file system properties.").withConfigProperty(configProperty);
      }
    }
  }

  /**
   * Returns a copy of the given user supplied file system properties without reserved keys. Reserved keys are
   * rejected at validation time; dropping them where the properties are copied into the Hadoop configuration as
   * well guarantees that values resolved from macros after validation cannot reach the connector either.
   *
   * @param properties user supplied file system properties, may be null
   * @return a new mutable map containing only the properties that are not reserved
   */
  public static Map<String, String> removeReservedFileSystemProperties(@Nullable Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>();
    if (properties == null) {
      return filtered;
    }
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (!isReservedFileSystemProperty(entry.getKey())) {
        filtered.put(entry.getKey(), entry.getValue());
      }
    }
    return filtered;
  }
}
