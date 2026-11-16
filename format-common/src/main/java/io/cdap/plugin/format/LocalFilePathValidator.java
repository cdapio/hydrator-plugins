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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Validates file paths and file system properties to prevent unauthorized local container file
 * system access, symlink traversal, and sensitive configuration or credential disclosure.
 */
public final class LocalFilePathValidator {

  /**
   * Configuration and system property that disables all local file system access in file plugins
   * when set to {@code true}.
   */
  public static final String LOCAL_FILE_DISABLED_PROPERTY =
      "io.cdap.plugin.format.local.file.disabled";

  private static final int MAX_SYMLINK_DEPTH = 10;

  private static final Set<String> RESTRICTED_FS_PROPERTIES = ImmutableSet.of(
      "fs.defaultfs",
      "fs.default.name",
      "fs.file.impl",
      "fs.abstractfilesystem.file.impl",
      "fs.local.impl",
      "fs.abstractfilesystem.local.impl",
      "fs.jar.impl",
      "fs.abstractfilesystem.jar.impl",
      LOCAL_FILE_DISABLED_PROPERTY
  );

  private static final List<java.nio.file.Path> RESTRICTED_LOCAL_PREFIXES = ImmutableList.of(
      Paths.get("/var/run/secrets"),
      Paths.get("/run/secrets"),
      Paths.get("/proc"),
      Paths.get("/sys"),
      Paths.get("/etc"),
      Paths.get("/root"),
      Paths.get("/dev"),
      Paths.get("/boot"),
      Paths.get("/var/lib/kubelet"),
      Paths.get("/var/lib/docker"),
      Paths.get("/var/lib/containerd"),
      Paths.get("/var/log")
  );

  private static final Set<String> RESTRICTED_FILENAMES = ImmutableSet.of(
      "cconf.xml",
      "hconf.xml",
      "cdap-site.xml",
      "cdap-security.xml",
      "core-site.xml",
      "hdfs-site.xml",
      "yarn-site.xml",
      "mapred-site.xml",
      "logback.xml",
      "logback-container.xml"
  );

  private static final List<String> RESTRICTED_EXTENSIONS = ImmutableList.of(
      ".jks",
      ".keystore",
      ".truststore",
      ".keytab",
      ".pem",
      ".p12",
      ".pfx"
  );

  private LocalFilePathValidator() {
  }

  /**
   * Validates user-supplied Hadoop file system properties to ensure security-sensitive FileSystem
   * implementation bindings cannot be overridden.
   *
   * @param properties map of Hadoop file system properties
   * @throws IllegalArgumentException if a restricted property is present
   */
  public static void validateFileSystemProperties(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return;
    }
    for (String key : properties.keySet()) {
      if (key != null
          && RESTRICTED_FS_PROPERTIES.contains(key.trim().toLowerCase(Locale.ROOT))) {
        throw new IllegalArgumentException(
            String.format("Overriding file system property '%s' is not allowed.", key));
      }
    }
  }

  /**
   * Validates a Hadoop {@link Path} against local file system security restrictions.
   *
   * @param hadoopPath the Hadoop path to validate
   * @param conf optional Hadoop configuration
   * @throws IllegalArgumentException if the path is invalid or references a restricted local path
   */
  public static void validatePath(@Nullable Path hadoopPath, @Nullable Configuration conf) {
    if (hadoopPath == null) {
      throw new IllegalArgumentException("File path cannot be null or empty.");
    }
    validateInternal(hadoopPath.toString(), hadoopPath.toUri(), conf);
  }

  /**
   * Validates a raw path string against local file system security restrictions.
   *
   * @param rawPath the raw path string to validate
   * @param conf optional Hadoop configuration
   * @throws IllegalArgumentException if the path is invalid or references a restricted local path
   */
  public static void validatePath(@Nullable String rawPath, @Nullable Configuration conf) {
    if (Strings.isNullOrEmpty(rawPath) || rawPath.trim().isEmpty()) {
      throw new IllegalArgumentException("File path cannot be null or empty.");
    }
    String trimmed = rawPath.trim();
    if (trimmed.regionMatches(true, 0, "jar:", 0, 4)) {
      throw new IllegalArgumentException(
          String.format("URI scheme 'jar' is not allowed in file path '%s'.", rawPath));
    }
    // Check raw string for parent traversal before Hadoop Path normalizes '..' segments.
    if ( isLocalSchemeOrSchemeless(trimmed, conf) && containsParentTraversal(trimmed)) {
      throw new IllegalArgumentException(
          String.format("File path '%s' cannot contain parent directory traversal ('..').",
              rawPath));
    }
    Path hadoopPath = new Path(trimmed);
    validateInternal(trimmed, hadoopPath.toUri(), conf);
  }

  private static void validateInternal(String rawPath, URI uri, @Nullable Configuration conf) {
    if (rawPath.regionMatches(true, 0, "jar:", 0, 4)) {
      throw new IllegalArgumentException(
          String.format("URI scheme 'jar' is not allowed in file path '%s'.", rawPath));
    }
    String scheme = uri.getScheme();
    if (scheme != null && "jar".equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException(
          String.format("URI scheme 'jar' is not allowed in file path '%s'.", rawPath));
    }

    if (!isLocalFileSystem(scheme, conf)) {
      return;
    }

    if (isLocalFileAccessDisabled(conf)) {
      throw new IllegalArgumentException(
          String.format("Local file system access is disabled for path '%s'.", rawPath));
    }

    String uriPath = uri.getPath();
    if (Strings.isNullOrEmpty(uriPath)) {
      uriPath = rawPath;
    }

    if (containsParentTraversal(rawPath) || containsParentTraversal(uriPath)) {
      throw new IllegalArgumentException(
          String.format("File path '%s' cannot contain parent directory traversal ('..').",
              rawPath));
    }

    validateLocalPath(uriPath, rawPath, 0);
  }

  private static boolean isLocalSchemeOrSchemeless(String rawPath, @Nullable Configuration conf) {
    int colonIdx = rawPath.indexOf(':');
    int slashIdx = rawPath.indexOf('/');
    if (colonIdx > 0 && (slashIdx < 0 || colonIdx < slashIdx)) {
      String scheme = rawPath.substring(0, colonIdx);
      return isLocalFileSystem(scheme, conf);
    }
    return isLocalFileSystem(null, conf);
  }

  private static boolean isLocalFileSystem(@Nullable String scheme, @Nullable Configuration conf) {
    if (scheme != null) {
      return "file".equalsIgnoreCase(scheme) || "local".equalsIgnoreCase(scheme);
    }
    if (conf == null) {
      return true;
    }
    String defaultFs = conf.get(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    try {
      String defaultScheme = new Path(defaultFs).toUri().getScheme();
      return defaultScheme == null
          || "file".equalsIgnoreCase(defaultScheme)
          || "local".equalsIgnoreCase(defaultScheme);
    } catch (Exception e) {
      return true;
    }
  }

  private static boolean isLocalFileAccessDisabled(@Nullable Configuration conf) {
    if (Boolean.getBoolean(LOCAL_FILE_DISABLED_PROPERTY)) {
      return true;
    }
    return conf != null && conf.getBoolean(LOCAL_FILE_DISABLED_PROPERTY, false);
  }

  private static boolean containsParentTraversal(String pathStr) {
    String decoded = decodePercentEncoded(pathStr);
    return hasDotDotSegment(pathStr) || hasDotDotSegment(decoded);
  }

  private static String decodePercentEncoded(String input) {
    if (input.indexOf('%') < 0) {
      return input;
    }
    try {
      return URLDecoder.decode(input, StandardCharsets.UTF_8.name());
    } catch (Exception e) {
      return input;
    }
  }

  private static boolean hasDotDotSegment(String input) {
    String normalizedSlashes = input.replace('\\', '/');
    String[] segments = normalizedSlashes.split("/");
    for (String segment : segments) {
      if ("..".equals(segment.trim())) {
        return true;
      }
    }
    return false;
  }

  private static void validateLocalPath(String localPathStr, String displayPath, int depth) {
    if (depth > MAX_SYMLINK_DEPTH) {
      throw new IllegalArgumentException(
          String.format("File path '%s' exceeds maximum symbolic link resolution depth.",
              displayPath));
    }

    checkSensitiveFileNames(localPathStr, displayPath);

    String globFreePath = stripGlobSuffix(localPathStr);
    java.nio.file.Path normalized;
    try {
      normalized = Paths.get(globFreePath).toAbsolutePath().normalize();
    } catch (InvalidPathException e) {
      throw new IllegalArgumentException(
          String.format("Invalid local file path '%s': %s", displayPath, e.getMessage()), e);
    }

    checkRestrictedPathPrefix(normalized, displayPath);
    checkSensitiveFileNames(normalized.toString(), displayPath);

    // Walk every prefix from root to normalized path to detect symbolic links (including
    // dangling symbolic links pointing to restricted container paths).
    java.nio.file.Path root = normalized.getRoot();
    if (root == null) {
      return;
    }
    java.nio.file.Path current = root;
    for (java.nio.file.Path part : normalized) {
      current = current.resolve(part);
      if (Files.isSymbolicLink(current)) {
        try {
          java.nio.file.Path target = Files.readSymbolicLink(current);
          java.nio.file.Path baseDir = current.getParent() != null ? current.getParent() : root;
          java.nio.file.Path resolvedTarget =
              baseDir.resolve(target).toAbsolutePath().normalize();
          checkRestrictedPathPrefix(resolvedTarget, displayPath);
          checkSensitiveFileNames(resolvedTarget.toString(), displayPath);

          java.nio.file.Path fullResolved = current.equals(normalized)
              ? resolvedTarget
              : resolvedTarget.resolve(current.relativize(normalized)).normalize();
          validateLocalPath(fullResolved.toString(), displayPath, depth + 1);
          return;
        } catch (IOException e) {
          throw new IllegalArgumentException(
              String.format("Failed to resolve symbolic link in path '%s'.", displayPath), e);
        }
      }
    }

    if (Files.exists(normalized)) {
      try {
        java.nio.file.Path realPath = normalized.toRealPath();
        checkRestrictedPathPrefix(realPath, displayPath);
        checkSensitiveFileNames(realPath.toString(), displayPath);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("Failed to resolve real path for '%s'.", displayPath), e);
      }
    }
  }

  private static void checkRestrictedPathPrefix(java.nio.file.Path normalizedPath,
                                                String displayPath) {
    for (java.nio.file.Path restricted : RESTRICTED_LOCAL_PREFIXES) {
      if (normalizedPath.startsWith(restricted)) {
        throw new IllegalArgumentException(
            String.format("Access to restricted local system path '%s' is not allowed.",
                displayPath));
      }
    }
  }

  private static void checkSensitiveFileNames(String pathStr, String displayPath) {
    String normalizedSlashes = pathStr.replace('\\', '/');
    String[] segments = normalizedSlashes.split("/");
    for (String segment : segments) {
      if (segment.isEmpty()) {
        continue;
      }
      String lower = segment.toLowerCase(Locale.ROOT);
      String lowerWithoutGlobs = lower.replace("*", "").replace("?", "");
      if (RESTRICTED_FILENAMES.contains(lower)
          || RESTRICTED_FILENAMES.contains(lowerWithoutGlobs)) {
        throw new IllegalArgumentException(
            String.format("Access to sensitive configuration file in path '%s' is not allowed.",
                displayPath));
      }
      for (String ext : RESTRICTED_EXTENSIONS) {
        if (lower.endsWith(ext) || lowerWithoutGlobs.endsWith(ext)) {
          throw new IllegalArgumentException(
              String.format("Access to sensitive credential file in path '%s' is not allowed.",
                  displayPath));
        }
      }
    }
  }

  private static String stripGlobSuffix(String pathStr) {
    int firstGlob = -1;
    for (int i = 0; i < pathStr.length(); i++) {
      char c = pathStr.charAt(i);
      if (c == '*' || c == '?' || c == '[' || c == '{') {
        firstGlob = i;
        break;
      }
    }
    if (firstGlob < 0) {
      return pathStr;
    }
    int lastSlash = pathStr.lastIndexOf('/', firstGlob);
    if (lastSlash <= 0) {
      return "/";
    }
    return pathStr.substring(0, lastSlash);
  }
}
