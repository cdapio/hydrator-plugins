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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Filters files based on regex.
 */
public class RegexFilter extends Configured implements PathFilter {
  private Pattern pattern;

  @Override
  public boolean accept(Path path) {
    Matcher m = pattern.matcher(path.toString());
    return m.matches();
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf == null) {
      return;
    }
    pattern = Pattern.compile(conf.get("file.pattern"));
  }
}
