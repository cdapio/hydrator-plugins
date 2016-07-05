/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.common.batch.action.ConditionConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;

/**
 * Apply action (Delete, Move or Archive) on file(s) at the end of pipeline.
 * The user must specify a path of file(s) on which action required, an action and a target folder if action is
 * either ARCHIVE or MOVE.
 * The user can specify the pattern to apply on file name to filter the files.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name("FileAction")
@Description("Apply action (Delete, Move or Archive) on file(s) at the end of pipeline.")
public class FileAction extends PostAction {
  private static final Logger LOG = LoggerFactory.getLogger(FileAction.class);

  public static final String DELETE_ACTION = "Delete";
  public static final String MOVE_ACTION = "Move";
  public static final String ARCHIVE_ACTION = "Archive";

  private final Config config;
  private Pattern regex;

  /**
   * Config for the file action plugin.
   */
  public static class Config extends ConditionConfig {
    @Description("Path to file(s) on which action required. " +
      "If a directory is specified, terminate the path name with a \'/\'.")
    private String path;

    @Description("Action to be taken on the file(s). " +
      "Possible actions are - " +
      "1. None - no action required." +
      "2. Delete - delete from the HDFS." +
      "3. Archive - archive to the target location." +
      "4. Moved - move to the target location.")
    private String action;

    @Nullable
    @Description("Target folder path if user select an action as either ARCHIVE or MOVE. " +
      "Target folder must be an existing directory.")
    private String targetFolder;

    @Nullable
    @Description("Pattern to select specific file(s)." +
      "Example - " +
      "1. Use '^' to select file starting with 'catalog', like '^catalog'." +
      "2. Use '$' to select file ending with 'catalog.xml', like 'catalog.xml$'." +
      "3. Use '*' to select file with name containing 'catalogBook', like 'catalogBook*'.")
    private final String pattern;

    @VisibleForTesting
    Config(String path, @Nullable String targetFolder, String action, String pattern) {
      super();
      this.path = path;
      this.targetFolder = targetFolder;
      this.action = action;
      this.pattern = pattern;
    }

    @VisibleForTesting
    String getPath() {
      return path;
    }

    @VisibleForTesting
    String getTargetFolder() {
      return targetFolder;
    }

    @VisibleForTesting
    String getAction() {
      return action;
    }

    public void validate() {
      super.validate();
      boolean targetFolderEmpty = (action.equals(ARCHIVE_ACTION) || action.equals(MOVE_ACTION))
        && Strings.isNullOrEmpty(targetFolder);
      Preconditions.checkArgument(!targetFolderEmpty, "Target folder cannot be Empty for Action = '" + action + "'.");
    }
  }

  public FileAction(Config config) {
    this.config = config;
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void run(BatchActionContext context) throws Exception {
    if (!config.shouldRun(context)) {
      return;
    }

    config.substituteMacros(context);

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    FileSystem fileSystem = FileSystem.get(conf);
    Path[] paths;
    Path sourcePath = new Path(config.path);
    if (fileSystem.isDirectory(sourcePath)) {
      FileStatus[] status = fileSystem.listStatus(sourcePath);
      paths = FileUtil.stat2Paths(status);
    } else {
      paths = new Path[]{sourcePath};
    }

    //Get regex pattern for file name filtering.
    if (!Strings.isNullOrEmpty(config.pattern)) {
      regex = Pattern.compile(config.pattern);
    }

    switch (config.action) {
      case DELETE_ACTION:
        for (Path path : paths) {
          if (isFileNameMatch(path.getName())) {
            fileSystem.delete(path, true);
          }
        }
        break;
      case MOVE_ACTION:
        for (Path path : paths) {
          if (isFileNameMatch(path.getName())) {
            Path targetFileMovePath = new Path(config.targetFolder, path.getName());
            fileSystem.rename(path, targetFileMovePath);
          }
        }
        break;
      case ARCHIVE_ACTION:
        for (Path path : paths) {
          if (isFileNameMatch(path.getName())) {
            try (FSDataOutputStream archivedStream = fileSystem.create(new Path(config.targetFolder,
                                                                                path.getName() + ".zip"));
                 ZipOutputStream zipArchivedStream = new ZipOutputStream(archivedStream);
                 FSDataInputStream fdDataInputStream = fileSystem.open(path)) {
              zipArchivedStream.putNextEntry(new ZipEntry(path.getName()));
              int length;
              byte[] buffer = new byte[1024];
              while ((length = fdDataInputStream.read(buffer)) > 0) {
                zipArchivedStream.write(buffer, 0, length);
              }
              zipArchivedStream.closeEntry();
            }
            fileSystem.delete(path, true);
          }
        }
        break;
      default:
        LOG.warn("No action required on the file.");
        break;
    }
  }

  private boolean isFileNameMatch(String fileName) {
    if (regex == null) {
      return true;
    }
    Matcher matcher = regex.matcher(fileName);
    return matcher.find();
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
  }
}
