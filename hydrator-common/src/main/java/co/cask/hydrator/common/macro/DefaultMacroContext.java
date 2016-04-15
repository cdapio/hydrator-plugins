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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import javax.annotation.Nullable;

/**
 * Default implementation of MacroContext.
 */
public class DefaultMacroContext implements MacroContext {
  private final long logicalStartTime;
  private final TimeZone timeZone;
  private final Map<String, String> runtimeArguments;

  public DefaultMacroContext(long logicalStartTime, TimeZone timeZone) {
    this(logicalStartTime, timeZone, new HashMap<String, String>());
  }

  public DefaultMacroContext(long logicalStartTime, TimeZone timeZone, Map<String, String> runtimeArguments) {
    this.logicalStartTime = logicalStartTime;
    this.timeZone = timeZone;
    this.runtimeArguments = Collections.unmodifiableMap(runtimeArguments);
  }

  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  @Nullable
  @Override
  public TimeZone getTimeZone() {
    return timeZone;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }
}
