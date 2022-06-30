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
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
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
   * @throws IOException
   */
  @Nullable
  public Schema detectSchema(String path, FormatContext formatContext,
                             Map<String, String> fileSystemProperties) throws IOException {
    if (path == null) {
      return null;
    }

    InputFiles inputFiles = getInputFiles(path, fileSystemProperties);
    return inputFormat.detectSchema(formatContext, inputFiles);
  }

  private InputFiles getInputFiles(String path, Map<String, String> fileSystemProperties) throws IOException {
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
    FileSystem fs = FileSystem.get(fsPath.toUri(), configuration);
    configuration.setClassLoader(cl);

    if (!fs.exists(fsPath)) {
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

    return new FileSystemInputFiles(fs, Arrays.asList(files));
  }
}
