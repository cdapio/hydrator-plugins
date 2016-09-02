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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Created by Abhinav on 8/30/16.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("Unc")
@Description("Batch source for File Systems")
public class UncBatchSource extends FileBatchSource {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {
  }.getType();
  private final UncBatchConfig uncBatchConfig;

  public UncBatchSource(UncBatchConfig uncBatchConfig) {
    super(new FileBatchConfig(uncBatchConfig.referenceName, uncBatchConfig.path, uncBatchConfig.fileRegex, null,
                              UncInputFormat.class.getName(), limitSplits(uncBatchConfig.fileSystemProperties), null));
    this.uncBatchConfig = uncBatchConfig;
  }

  private static String limitSplits(@Nullable String fsProperties) {
    Map<String, String> providedProperties;
    if (fsProperties == null) {
      providedProperties = new HashMap<>();
    } else {
      providedProperties = GSON.fromJson(fsProperties, MAP_STRING_STRING_TYPE);
    }
    providedProperties.put(FileInputFormat.SPLIT_MINSIZE, Long.toString(Long.MAX_VALUE));
    return GSON.toJson(providedProperties);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {

      Job job = JobUtils.createInstance();
      Configuration conf = job.getConfiguration();

      conf.set(INPUT_REGEX_CONFIG, uncBatchConfig.fileRegex);
      conf.set(INPUT_NAME_CONFIG, uncBatchConfig.path);
      UncInputFormat.setConfigurations(job, uncBatchConfig.user, uncBatchConfig.password, uncBatchConfig.uncpath);

      FileInputFormat.addInputPath(job, new Path(uncBatchConfig.path));



      context.setInput(Input.of(uncBatchConfig.referenceName, new SourceInputFormatProvider(UncInputFormat.class,
                                                                                    conf)));
    }

  /**
   *
   */
  public static class UncBatchConfig extends FTPBatchSource.FTPBatchSourceConfig {
    @Nullable
    @Name("username")
    @Description("username")
    public String user;

    @Nullable
    @Name("uncpath")
    @Description("uncpath")
    public String uncpath;

    @Nullable
    @Description("password")
    @Name("password")
    public String password;

    public UncBatchConfig(String referenceName, String path, @Nullable String fileSystemProperties,
                          @Nullable String fileRegex, @Nullable String inputFormatClassName) {
      super(referenceName, path, fileSystemProperties, fileRegex, inputFormatClassName);
    }

    public UncBatchConfig(String referenceName, String path, @Nullable String fileSystemProperties, String uncpath,
                          @Nullable String fileRegex, @Nullable String inputFormatClassName, String user,
                          String password) {
      super(referenceName, path, fileSystemProperties, fileRegex, inputFormatClassName);
      this.user = user;
      this.password = password;
      this.uncpath = uncpath;
    }
  }
}
