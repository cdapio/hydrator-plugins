/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin.common;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.plugin.common.batch.ConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

import java.util.Map;

/**
 * An implementation of {@link InputFormatProvider} based on {@link Configuration} and input format class.
 */
public final class SourceInputFormatProvider implements InputFormatProvider {

  private final String inputFormatClassName;
  private final Map<String, String> configuration;

  public SourceInputFormatProvider(Class<? extends InputFormat> inputFormatClass, Configuration hConf) {
    this(inputFormatClass.getName(), hConf);
  }

  public SourceInputFormatProvider(String inputFormatClassName, Configuration hConf) {
    this.inputFormatClassName = inputFormatClassName;
    this.configuration = ConfigurationUtils.getNonDefaultConfigurations(hConf);
  }

  @Override
  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return configuration;
  }
}
