/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * A PathFilter that only allows files whose name matches a specific regex.
 */
public class RegexPathFilter extends Configured implements PathFilter {
  private static final String REGEX = "path.filter.regex";
  private Pattern pattern;

  public static void configure(Configuration conf, Pattern regex) {
    conf.set(REGEX, regex.pattern());
  }

  @Override
  public boolean accept(Path path) {
    try {
      FileSystem fileSystem = path.getFileSystem(getConf());
      if (fileSystem.isDirectory(path)) {
        return true;
      } else if (fileSystem.isFile(path)) {
        return pattern == null || pattern.matcher(path.toUri().getPath()).matches();
      }
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      return;
    }
    String regex = conf.get(REGEX);
    pattern = regex == null ? null : Pattern.compile(regex);
  }
}
