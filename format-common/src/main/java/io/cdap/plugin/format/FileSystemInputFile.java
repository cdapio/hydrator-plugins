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

import io.cdap.cdap.etl.api.validation.InputFile;
import io.cdap.cdap.etl.api.validation.SeekableInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * InputFile that comes from a Hadoop FileSystem.
 */
public class FileSystemInputFile implements InputFile {
  private final FileSystem fs;
  private final Path path;
  private final long length;

  public FileSystemInputFile(FileSystem fs, FileStatus file) {
    this.fs = fs;
    this.path = file.getPath();
    this.length = file.getLen();
  }

  @Override
  public String getName() {
    return path.getName();
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public SeekableInputStream open() throws IOException {
    return new FileSystemSeekableInputStream(fs.open(path));
  }
}
