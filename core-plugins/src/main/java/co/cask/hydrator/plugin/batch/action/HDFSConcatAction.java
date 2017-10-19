package co.cask.hydrator.plugin.batch.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

public class HDFSConcatAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSConcatAction.class);

  private HDFSActionConfig config;

  public HDFSConcatAction(HDFSActionConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Path source = new Path(config.sourcePath);

    Path dest = new Path(config.destPath);

    FileSystem fileSystem = source.getFileSystem(new Configuration());
    fileSystem.mkdirs(dest.getParent());

    if (fileSystem.getFileStatus(source).isFile()) { //moving single file
      LOG.error("Failed to concatenate, source path {} is a file ", source.toString());
      if (!config.continueOnError) {
        throw new IOException(String.format("Expected path '%s' to directory, but it is a file'",
                                            source.toString()));
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
        LOG.warn("Not concatenating any files of type {} from source {}", config.fileRegex, source.toString());
      } else {
        LOG.warn("Not concatenating any files from source {}", source.toString());
      }
    }

    if (fileSystem.isFile(dest)) {
      throw new IllegalArgumentException(String.format("destPath %s needs to be a directory since sourcePath is a " +
                                                         "directory", config.destPath));
    }
    fileSystem.mkdirs(dest); //create destination directory if necessary

    // order the files
    Arrays.sort(listFiles, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        String path1 = o1.getPath().getName();
        String path2 = o2.getPath().getName();
        int prefixIndex1 = path1.lastIndexOf("-");
        int prefixIndex2 = path2.lastIndexOf("-");
        int compareVal = path1.substring(0, prefixIndex1).compareTo(path2.substring(0, prefixIndex2));
        if (compareVal == 0) {
          Integer.compare(Integer.parseInt(path1.substring(prefixIndex1 + 1)),
                          Integer.parseInt(path2.substring(prefixIndex2 + 1)));
        } else {
          return compareVal;
        }
        return 0;
      }
    });

    byte[] resultByteArray = new byte[0];

    for (FileStatus file: listFiles) {
      source = file.getPath();
      try {
        Bytes.add(resultByteArray, FileUtils.readFileToByteArray(new File(file.getPath().toUri())));
      } catch (IOException e) {
        if (!config.continueOnError) {
          throw e;
        }
        LOG.error("Failed to concatenate file {} to {}", source.toString(), dest.toString(), e);
      }
    }
    try {
      if (resultByteArray.length > 0) {
        String path = String.format("%s/%s", config.destPath, "output");
        FSDataOutputStream outputStream = fileSystem.create(new Path(path));
        outputStream.write(resultByteArray);
        outputStream.close();
      }
    } catch (IOException e) {
      if (!config.continueOnError) {
        throw e;
      }
      LOG.error("Failed to concatenate file {} to {}", source.toString(), dest.toString(), e);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {

  }

  /**
   * Config class that contains all properties necessary to execute an HDFS move command.
   */
  public class HDFSActionConfig extends PluginConfig {
    @Description("The full HDFS path of the directory whose content have to be concatenated. " +
      "if fileRegex is set, then only files in the source directory matching the wildcard regex will be concatenated." +
      "Otherwise, all files in the directory will be concatenated. For example: hdfs://hostname/tmp")
    @Macro
    private String sourcePath;

    @Description("The valid, full HDFS destination path in the same cluster where the concatenated file will be moved.")
    @Macro
    private String destPath;

    @Description("Wildcard regular expression to filter the files in the source directory that will be moved")
    @Nullable
    private String fileRegex;

    @Description("Indicates if the pipeline should continue if the concatenate process fails")
    private boolean continueOnError;

    @VisibleForTesting
    HDFSActionConfig(String sourcePath, String destPath, String fileRegex, boolean continueOnError) {
      this.sourcePath = sourcePath;
      this.destPath = destPath;
      this.fileRegex = fileRegex;
      this.continueOnError = continueOnError;
    }
  }
}
