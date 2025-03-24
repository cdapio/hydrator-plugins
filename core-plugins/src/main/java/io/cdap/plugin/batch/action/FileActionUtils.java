/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.plugin.batch.action;

import io.cdap.plugin.batch.source.FileErrorDetailsProvider;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Utility class for common file action operations.
 */
public class FileActionUtils {

  static FileStatus[] getFileStatuses(FileSystem fileSystem, Path path, @Nullable PathFilter filter) {
    try {
      if (filter == null) {
        return fileSystem.listStatus(path);
      }
      return fileSystem.listStatus(path, filter);
    } catch (IOException e) {
      String errorReason = String.format("Failed to list files in %s.", path);
      throw FileErrorDetailsProvider.getFileBasedProgramFailureExceptionDetailsFromChain(e, errorReason);
    }
  }
}
