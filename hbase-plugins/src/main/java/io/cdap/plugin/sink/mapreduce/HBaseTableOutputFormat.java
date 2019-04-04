/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.plugin.sink.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

/**
 * A wrapper class around TableOutputFormat, that sets the current class's classloader as the classloader of the
 * Configuration object used by TableOutputFormat.
 *
 * @param <KEY> Type of Key
 */
public class HBaseTableOutputFormat<KEY> extends TableOutputFormat<KEY> {

  @Override
  public void setConf(Configuration otherConf) {
    // To resolve CDAP-12731, set the current class's classloader to the thread's context classloader,
    // so that it gets picked up when the super.setConf() calls HBaseConfiguration.create(Configuration)
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      super.setConf(otherConf);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }
}
