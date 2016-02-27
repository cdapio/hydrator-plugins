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

import javax.annotation.Nullable;

/**
 * Performs macro substitution at pipeline runtime.
 *
 * Macros are of the format ${macro-type(macro-arguments)}.
 * For example, ${runtime(1d,yyyy-MM-dd)} is a macro with macro-type 'runtime' and macro-arguments '1d,yyyy-MM-dd'.
 *
 * The '{' and '}' are treated as special characters and cannot be used in the macro-type or macro-arguments.
 * The exact format of a macro's arguments is left up to each individual macro.
 */
public interface Macro {

  /**
   * Get the value of the macro based on the context and arguments.
   *
   * @param arguments arguments to the macro
   * @param context the macro context, which gives access to things like the logical start time and runtime arguments.
   * @return the macro value
   */
  String getValue(@Nullable String arguments, MacroContext context) throws Exception;
}
