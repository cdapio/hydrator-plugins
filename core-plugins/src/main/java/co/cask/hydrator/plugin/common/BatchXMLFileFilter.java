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

package co.cask.hydrator.plugin.common;

import co.cask.hydrator.plugin.batch.source.XMLInputFormat;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Filter class to filter out XML filenames in the input path.
 */
public class BatchXMLFileFilter extends Configured implements PathFilter {
  private static final Gson GSON = new Gson();
  private static final Type ARRAYLIST_PREPROCESSED_FILES = new TypeToken<ArrayList<String>>() { }.getType();
  private Pattern regex;
  private String pathName;

  private List<String> preProcessedFileList;

  @Override
  public boolean accept(Path path) {
    String filePathName = path.toString();
    //The path filter will first check the directory if a directory is given
    if (filePathName.equals(pathName)) {
      return true;
    }
    Matcher matcher = regex.matcher(path.getName());
    boolean patternMatch = matcher.find();
    if (patternMatch && CollectionUtils.isNotEmpty(preProcessedFileList)) {
      patternMatch = !preProcessedFileList.contains(filePathName);
    }
    return patternMatch;
  }

  @Override
  public void setConf(@Nullable Configuration conf) {
    if (conf == null) {
      return;
    }
    pathName = conf.get(XMLInputFormat.XML_INPUTFORMAT_PATH_NAME, "/");

    //path is a directory so remove trailing '/'
    if (pathName.endsWith("/")) {
      pathName = pathName.substring(0, pathName.length() - 1);
    }

    String input = conf.get(XMLInputFormat.XML_INPUTFORMAT_PATTERN, ".*");
    regex = Pattern.compile(input);

    String processedFiles = conf.get(XMLInputFormat.XML_INPUTFORMAT_PROCESSED_FILES);
    if (!Strings.isNullOrEmpty(processedFiles)) {
      preProcessedFileList = GSON.fromJson(processedFiles, ARRAYLIST_PREPROCESSED_FILES);
    }
  }
}
