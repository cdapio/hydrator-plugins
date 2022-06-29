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
import io.cdap.cdap.etl.api.validation.InputFiles;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Supplies an InputStream of data to detect schema from.
 *
 * It implements Function as a hack to pass the file length down to the Parquet format, which needs it to seek to
 * the file footer.
 */
public class FileSystemInputFiles implements InputFiles {
  private final List<InputFile> files;

  public FileSystemInputFiles(FileSystem fs, List<FileStatus> files) {
    this.files = files.stream()
      .filter(FileStatus::isFile)
      .map(f -> new FileSystemInputFile(fs, f))
      .collect(Collectors.toList());
  }

  @Override
  public Iterator<InputFile> iterator() {
    return files.iterator();
  }
}
