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

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Filter class to filter out Excel filenames in the input path.
 */
public class ExcelReaderRegexFilter extends Configured implements PathFilter {

  private static final Logger LOG = LoggerFactory.getLogger(ExcelReaderRegexFilter.class);
  private static final String FILE_PATTERN = "filePattern";
  private static final String RE_PROCESS = "reprocess";
  private static final String PROCESSED_FILES = "processedFiles";
  private static final Gson GSON = new Gson();
  private static final Type ARRAYLIST_PREPROCESSED_FILES = new TypeToken<ArrayList<String>>() { }.getType();

  private Pattern pattern;
  private Configuration conf;
  private List<String> preProcessedFileList;

  @Override
  public boolean accept(Path path) {
    try {
      FileSystem fs = FileSystem.get(path.toUri(), conf);
      if (fs.isDirectory(path)) {
        return true;
      }

      boolean patternMatch = true;
      Matcher matcher = pattern.matcher(path.toString());
      patternMatch = matcher.find();
      if (patternMatch && !conf.getBoolean(RE_PROCESS, false) && CollectionUtils.isNotEmpty(preProcessedFileList)) {
        patternMatch = !preProcessedFileList.contains(path.toString());
      }

      return patternMatch;
    } catch (IOException e) {
      LOG.warn("Got error while testing path {}, so skipping it.", path, e);
      return false;
    }
  }

  @Override
  public void setConf(@Nullable Configuration conf) {
    this.conf = conf;
    if (conf == null) {
      return;
    }
    pattern = Pattern.compile(conf.get(FILE_PATTERN));
    String processedFiles = conf.get(PROCESSED_FILES);
    if (!Strings.isNullOrEmpty(processedFiles)) {
      preProcessedFileList = GSON.fromJson(processedFiles, ARRAYLIST_PREPROCESSED_FILES);
    }
  }
}
