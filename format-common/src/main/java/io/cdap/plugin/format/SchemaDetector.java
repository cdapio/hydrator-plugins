/*
 * Copyright © 2022 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.InputFiles;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Performs schema detection on a file path with a given input format.
 */
public class SchemaDetector {
  private final ValidatingInputFormat inputFormat;

  public SchemaDetector(ValidatingInputFormat inputFormat) {
    this.inputFormat = inputFormat;
  }

  /**
   * Detect the schema for files at a specified path
   *
   * @param path path to the files. Can be a file or a directory
   * @param formatContext format context
   * @param fileSystemProperties any properties that need to be set in order to read from the Hadoop FileSystem
   * @return the schema of files at the path, or null if the schema is not known.
   * @throws IOException if there was an exception interacting with the filesystem
   */
  @Nullable
  public Schema detectSchema(String path, FormatContext formatContext,
                             Map<String, String> fileSystemProperties) throws IOException {
    return detectSchema(path, null, formatContext, fileSystemProperties);
  }

  /**
   * Detect the schema for files at a specified path, where files need to match the specified pattern.
   *
   * @param path path to the files. Can be a file or a directory
   * @param pattern pattern that the files much match in order to be considered for schema detection. Files that do not
   *                match this pattern will be skipped. If none is given, any file can be used for schema detection.
   * @param formatContext format context
   * @param fileSystemProperties any properties that need to be set in order to read from the Hadoop FileSystem
   * @return the schema of files at the path, or null if the schema is not known.
   * @throws IOException if there was an exception interacting with the filesystem
   */
  @Nullable
  public Schema detectSchema(String path, @Nullable Pattern pattern, FormatContext formatContext,
                             Map<String, String> fileSystemProperties) throws IOException {
    if (path == null) {
      return null;
    }

    InputFiles inputFiles = getInputFiles(path, pattern, fileSystemProperties);
    return inputFormat.detectSchema(formatContext, inputFiles);
  }

  private InputFiles getInputFiles(String path, @Nullable Pattern pattern,
                                   Map<String, String> fileSystemProperties) throws IOException {
    Job job = JobUtils.createInstance();
    Configuration configuration = job.getConfiguration();
    for (Map.Entry<String, String> entry : fileSystemProperties.entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : inputFormat.getInputFormatConfiguration().entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }

    Path fsPath = new Path(path);

    ClassLoader cl = configuration.getClassLoader();
    configuration.setClassLoader(getClass().getClassLoader());
    try {
      FileSystem fs = FileSystem.get(fsPath.toUri(), configuration);

      PathFilter pathFilter = getPathFilter(pattern, configuration);
      if (!fs.exists(fsPath) || !pathFilter.accept(fsPath)) {
        throw new IOException("Input path not found");
      }

      FileStatus fileStatus = fs.getFileStatus(fsPath);
      if (fileStatus.isFile()) {
        return new FileSystemInputFiles(fs, Collections.singletonList(fileStatus));
      }

      FileStatus[] files = fs.listStatus(fsPath);

      if (files == null) {
        throw new IllegalArgumentException("Cannot read files from provided path " + path);
      }

      if (files.length == 0) {
        throw new IllegalArgumentException("Provided directory '" + path + "' is empty");
      }

      List<FileStatus> filteredFiles = Arrays.stream(files)
        .filter(fStatus -> pathFilter.accept(fStatus.getPath()))
        .collect(Collectors.toList());

      return new FileSystemInputFiles(fs, filteredFiles);
    } finally {
      configuration.setClassLoader(cl);
    }
  }

  private PathFilter getPathFilter(@Nullable Pattern pattern, Configuration configuration) {
    if (pattern == null) {
      return p -> true;
    }
    RegexPathFilter.configure(configuration, pattern);
    RegexPathFilter regexPathFilter = new RegexPathFilter();
    regexPathFilter.setConf(configuration);
    return regexPathFilter;
  }
}
