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

package co.cask.hydrator.common.macro;

import co.cask.hydrator.common.TimeParser;

import java.text.SimpleDateFormat;
import java.util.Date;
import javax.annotation.Nullable;

/**
 * Runtime macros use the logical start time of a run to perform substitution.
 * Runtime macros follow the syntax ${runtime(arguments)}. Arguments are expected to be either:
 *
 * empty string
 * format
 * format,offset
 *
 * If no format is given, the runtime in milliseconds will be used.
 * Otherwise, the format is expected to be a SimpleDateFormat that will be used to format the runtime.
 * The offset can be used to specify some amount of time to subtract from the runtime before formatting it.
 * The offset must be parse-able by {@link TimeParser}, which allows some simple math expressions.
 * For example, 1d-4h means 20 hours before the runtime.
 */
public class RuntimeMacro implements Macro {

  @Override
  public String getValue(@Nullable String arguments, MacroContext context) {
    long runtime = context.getLogicalStartTime();
    if (arguments == null || arguments.isEmpty()) {
      return String.valueOf(runtime);
    }

    long offset = 0;
    String format = arguments;
    int commaIndex = arguments.lastIndexOf(',');
    if (commaIndex > 0) {
      TimeParser timeParser = new TimeParser(runtime);
      offset = timeParser.parseRuntime(arguments.substring(commaIndex + 1));
      format = arguments.substring(0, commaIndex);
    }

    Date date = new Date(runtime - offset);
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
    if (context.getTimeZone() != null) {
      simpleDateFormat.setTimeZone(context.getTimeZone());
    }
    return simpleDateFormat.format(date);
  }
}
