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

package io.cdap.plugin.source.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

/**
 * A wrapper class around TableInputFormat, that sets the current class's classloader as the classloader of the
 * Configuration object used by TableInputFormat.
 */
public class HBaseTableInputFormat extends TableInputFormat {

  @Override
  public void setConf(Configuration otherConf) {
    // To resolve CDAP-12731, set the current class's classloader to the Configuration object that is used
    // Note that the approach here is different than in HBaseTableOutputFormat, due to how the parent classes
    // use the Configuration object passed in
    Configuration clonedConf = new Configuration(otherConf);
    clonedConf.setClassLoader(getClass().getClassLoader());
    super.setConf(clonedConf);
  }
}
