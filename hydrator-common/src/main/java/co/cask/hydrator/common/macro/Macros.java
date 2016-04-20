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

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Creates the correct type of macro.
 *
 * Macros are of the format ${macro-type(macro-arguments)}.
 * For example, ${runtime(yyyy-MM-dd,1d)} is a macro with macro-type 'runtime' and macro-arguments 'yyyy-MM-dd,1d'.
 * This class would create a {@link RuntimeMacro}.
 */
public class Macros {
  private static final Map<String, Macro> MACRO_MAP = ImmutableMap.<String, Macro>of("runtime", new RuntimeMacro());

  @Nullable
  public static Macro fromType(String type) {
    return MACRO_MAP.get(type);
  }
}
