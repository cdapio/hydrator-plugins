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
 * A class specifying a DateTime standard and its regex patterns.
 */
public class DateTimeStandard {
  private String standardName;
  private List<String> regex;
  private String comment;

  public DateTimeStandard() {}

  public DateTimeStandard(String standardName, List<String> regex, String comment) {
    this.standardName = standardName;
    this.regex = regex;
    this.comment = comment;
  }

  public String getStandardName() {
        return standardName;
    }

  public void setStandardName(String standardName) {
        this.standardName = standardName;
    }

  public List<String> getRegex() {
        return regex;
    }

  public void setRegex(List<String> regex) {
        this.regex = regex;
    }

  public String getComment() {
        return comment;
    }

  public void setComment(String comment) {
        this.comment = comment;
    }
}

