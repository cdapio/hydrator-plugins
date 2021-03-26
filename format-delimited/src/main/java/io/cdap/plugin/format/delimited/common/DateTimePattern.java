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

package io.cdap.plugin.format.delimited.common;

import com.google.gson.Gson;
import io.cdap.plugin.format.delimited.dto.DateTimeStandard;
import io.cdap.plugin.format.delimited.dto.SupportedDateTimeStandards;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Date time patterns
 */
public class DateTimePattern {
  private static final List<Pattern> DATE_PATTERNS = new ArrayList<>();
  private static final List<Pattern> TIME_PATTERNS = new ArrayList<>();
  private static final String NAME_DATETIME_REGEX_FILE = "datetime-regex.json";
  private static final String NAME_TIME_REGEX_FILE = "time-regex.json";

  static {
    SupportedDateTimeStandards datetimeSupportedStandards = parseRegexFile(NAME_DATETIME_REGEX_FILE);
    SupportedDateTimeStandards timeSupportedStandards = parseRegexFile(NAME_TIME_REGEX_FILE);
    createPatterns(datetimeSupportedStandards, DATE_PATTERNS);
    createPatterns(timeSupportedStandards, TIME_PATTERNS);
  }

  /**
   * Checks if the given string value is a date or not.
   *
   * @param value The given raw string value.
   * @return True if the given value is of type "DATE", false otherwise.
   */
  public static boolean isDate(String value) {
    if (StringUtils.isEmpty(value)) {
      return false;
    }

    return isValueMatchingPattern(value, DATE_PATTERNS);
  }

  /**
   * Check if the value passed is a time or not.
   *
   * @param value The given raw string value.
   * @return true If the given value is of type "TIME", false otherwise.
   */
  public static boolean isTime(String value) {
    if (StringUtils.isEmpty(value)) {
      return false;
    }
    return isValueMatchingPattern(value, TIME_PATTERNS);
  }

  /**
   * Checks whether a given value matches one of the provided patterns.
   *
   * @param value A string value.
   * @param patterns A list of patterns.
   *
   * @return true If value matches one any pattern, false otherwise.
   */
  private static boolean isValueMatchingPattern(String value, List<Pattern> patterns) {
     for (Pattern pattern : patterns) {
          try {
            if (pattern.matcher(value).find()) {
              return true;
            }
          } catch (Exception e) {
            // do nothing
          }
        }
    return false;
  }

  /**
   * Parses and maps json contents of regex files to a {@link SupportedDateTimeStandards} object.
   *
   * @param fileName The name of the fail that will get parsed.
   * @return A {@link SupportedDateTimeStandards} consisting a collection of all supported date time standards.
   */
  private static SupportedDateTimeStandards parseRegexFile(String fileName) {
    Gson gson = new Gson();
    InputStream is = DateTimePattern.class.getResourceAsStream("/" + fileName);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    SupportedDateTimeStandards supportedStandards = gson.fromJson(reader, SupportedDateTimeStandards.class);
    return supportedStandards;
  }

  /**
   * Creates patterns out of the give standard regexes.
   *
   * @param supportedDateTimeStandards A collection of supported date time standards.
   * @param globalPatternList An outside collection that gets populated with patterns.
   */
  private static void createPatterns(SupportedDateTimeStandards supportedDateTimeStandards,
                                     List<Pattern> globalPatternList)  {
    for (DateTimeStandard dateTimeStandard : supportedDateTimeStandards.getSupportedStandards()) {
      for (String regex: dateTimeStandard.getRegex()) {
        globalPatternList.add(Pattern.compile(regex));
      }
    }
  }
}
