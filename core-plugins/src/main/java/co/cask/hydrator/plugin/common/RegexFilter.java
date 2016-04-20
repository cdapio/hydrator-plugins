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

import co.cask.hydrator.plugin.batch.source.FileSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Filters files based on regex.
 */
public class RegexFilter extends Configured implements PathFilter {
  private Pattern pattern;
  private FileSystem fs;
  private Configuration conf;

  @Override
  public boolean accept(Path path) {
    try {
      if (fs.isDirectory(path)) {
        return true;  
      }
      return pattern.matcher(path.toString()).matches();
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (conf == null) {
      return;
    }
    try {
      fs = FileSystem.get(conf);
      pattern = Pattern.compile(conf.get(FileSource.FILE_PATTERN));
    } catch (IOException e) {
    }
  }
}
