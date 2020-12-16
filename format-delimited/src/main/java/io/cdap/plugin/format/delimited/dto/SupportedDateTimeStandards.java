/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.format.delimited.dto;

import java.util.List;

/**
 * A class specifying a list of supported date time standard in the automated schema detection.
 */
public class SupportedDateTimeStandards {
  private List<DateTimeStandard> supportedStandards;

  public SupportedDateTimeStandards() {
  }

  public SupportedDateTimeStandards(List<DateTimeStandard> supportedStandards) {
      this.supportedStandards = supportedStandards;
  }

  public List<DateTimeStandard> getSupportedStandards() {
      return supportedStandards;
  }

  public void setSupportedStandards(List<DateTimeStandard> supportedStandards) {
      this.supportedStandards = supportedStandards;
  }
}
