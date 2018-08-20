/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Action that moves file(s).
 * A user must specify file/directory path and destination file/directory path
 * Optionals include fileRegex
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("FileMove")
@Description("Action to move files.")
public class FileMoveAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(FileMoveAction.class);

  private Conf config;

  public FileMoveAction(Conf config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Path source = new Path(config.sourcePath);

    Path dest = new Path(config.destPath);

    FileSystem fileSystem = source.getFileSystem(new Configuration());
    fileSystem.mkdirs(dest.getParent());

    if (fileSystem.getFileStatus(source).isFile()) { //moving single file

      try {
        if (!fileSystem.rename(source, dest)) {
          if (!config.continueOnError) {
            throw new IOException(String.format("Failed to rename file %s to %s", source, dest));
          }
          LOG.error("Failed to move file {} to {}", source, dest);
        }
      } catch (IOException e) {
        if (!config.continueOnError) {
          throw e;
        }
        LOG.error("Failed to move file {} to {}", source, dest, e);
      }
      return;
    }

    // Moving contents of directory
    FileStatus[] listFiles;
    if (config.fileRegex != null) {
      PathFilter filter = new PathFilter() {
        private final Pattern pattern = Pattern.compile(config.fileRegex);

        @Override
        public boolean accept(Path path) {
          return pattern.matcher(path.getName()).matches();
        }
      };

      listFiles = fileSystem.listStatus(source, filter);
    } else {
      listFiles = fileSystem.listStatus(source);
    }

    if (listFiles.length == 0) {
      if (config.fileRegex != null) {
        LOG.warn("Not moving any files of type {} from source {}", config.fileRegex, source.toString());
      } else {
        LOG.warn("Not moving any files from source {}", source.toString());
      }
    }

    if (fileSystem.isFile(dest)) {
      throw new IllegalArgumentException(String.format("destPath %s needs to be a directory since sourcePath is a " +
                                                         "directory", config.destPath));
    }
    fileSystem.mkdirs(dest); //create destination directory if necessary

    for (FileStatus file: listFiles) {
      source = file.getPath();

      try {
        if (!fileSystem.rename(source, dest)) {
          if (!config.continueOnError) {
            throw new IOException(String.format("Failed to rename file %s to %s", source, dest));
          }
          LOG.error("Failed to move file {} to {}", source, dest);
        }
      } catch (IOException e) {
        if (!config.continueOnError) {
          throw e;
        }
        LOG.error("Failed to move file {} to {}", source, dest, e);
      }
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
  }

  /**
   * Config class that contains all properties necessary to execute a file move command.
   */
  public class Conf extends PluginConfig {
    @Description("The full HDFS path of the file or directory that is to be moved. In the case of a directory, if " +
      "fileRegex is set, then only files in the source directory matching the wildcard regex will be moved. " +
      "Otherwise, all files in the directory will be moved. For example: hdfs://hostname/tmp")
    @Macro
    private String sourcePath;

    @Description("The valid, full HDFS destination path in the same cluster where the file or files are to be moved. " +
      "If a directory is specified with a file sourcePath, the file will be put into that directory. If sourcePath " +
      "is a directory, it is assumed that destPath is also a directory. HDFSAction will not catch this inconsistency.")
    @Macro
    private String destPath;

    @Description("Wildcard regular expression to filter the files in the source directory that will be moved")
    @Nullable
    @Macro
    private String fileRegex;

    @Description("Indicates if the pipeline should continue if the move process fails")
    private boolean continueOnError;

    public void validate() {
      if (!containsMacro("fileRegex") && fileRegex != null) {
        try {
          Pattern.compile(fileRegex);
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format("File regex %s is invalid: %s",
                                                           fileRegex, e.getMessage()), e);
        }
      }
    }

    @VisibleForTesting
    Conf(String sourcePath, String destPath, String fileRegex, boolean continueOnError) {
      this.sourcePath = sourcePath;
      this.destPath = destPath;
      this.fileRegex = fileRegex;
      this.continueOnError = continueOnError;
    }
  }
}
