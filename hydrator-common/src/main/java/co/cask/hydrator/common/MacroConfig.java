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

package co.cask.hydrator.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.batch.BatchContext;
import com.google.common.base.Strings;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import javax.annotation.Nullable;

/**
 * A type of plugin config that supports macro substitution.
 */
public abstract class MacroConfig extends PluginConfig {
  private static final String RUNTIME_PREFIX = "${runtime:";

  @Nullable
  @Description("The timezone to use for macro substitution. " +
    "Macros are of the format ${runtime.format}, " +
    "where format is a date format as described by Java's SimpleDateFormat. " +
    "For example, ${runtime:yyyy} will be replaced by the year of the runtime.")
  protected String macroTimeZone;

  @Nullable
  @Description("Some offset to subtract from the runtime for macro substitution. " +
    "The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' " +
    "for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, a value of '5m' means five minutes")
  protected String macroTimeOffset;

  public MacroConfig() {
  }

  protected MacroConfig(String macroTimeOffset, String macroTimeZone) {
    this.macroTimeZone = macroTimeZone;
    this.macroTimeOffset = macroTimeOffset;
  }

  /**
   * Validate that config fields are valid.
   *
   * @throws IllegalArgumentException if there are invalid config fields.
   */
  public void validate() throws IllegalArgumentException {
    ETLTime.parseDuration(macroTimeOffset);
  }

  /**
   * Performs macro substitution on all non-static string fields.
   *
   * @param context context of the pipeline run
   */
  public void substituteMacros(BatchContext context) {
    Calendar calendar = Strings.isNullOrEmpty(macroTimeZone) ?
      Calendar.getInstance() : Calendar.getInstance(TimeZone.getTimeZone(macroTimeZone));

    long runtime = ETLTime.getRuntime(context);
    if (!Strings.isNullOrEmpty(macroTimeOffset)) {
      runtime -= ETLTime.parseDuration(macroTimeOffset);
    }
    calendar.setTimeInMillis(runtime);
    TimeZone timeZone = macroTimeZone == null ? null : TimeZone.getTimeZone(macroTimeZone);

    for (Field field : getClass().getDeclaredFields()) {
      // skip static fields
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      if (field.getType() == String.class) {
        try {
          field.setAccessible(true);
          field.set(this, substitute((String) field.get(this), runtime, timeZone));
        } catch (IllegalAccessException e) {
          // can't do anything, just ignore
        }
      }
    }
  }

  private String substitute(String str, long runtime, @Nullable TimeZone timeZone) {
    Date date = new Date(runtime);
    StringBuilder builder = new StringBuilder();
    int currIndex = 0;
    int macroStartIndex;
    while ((macroStartIndex = str.indexOf(RUNTIME_PREFIX, currIndex)) >= 0) {
      builder.append(str.substring(currIndex, macroStartIndex));
      int macroEndIndex = str.indexOf('}', macroStartIndex + 1);
      if (macroEndIndex < 0) {
        break;
      }
      String pattern = str.substring(macroStartIndex + RUNTIME_PREFIX.length(), macroEndIndex);
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
      if (timeZone != null) {
        simpleDateFormat.setTimeZone(timeZone);
      }
      currIndex = macroEndIndex + 1;
      builder.append(simpleDateFormat.format(date));
    }
    builder.append(str.substring(currIndex));
    return builder.toString();
  }
}
