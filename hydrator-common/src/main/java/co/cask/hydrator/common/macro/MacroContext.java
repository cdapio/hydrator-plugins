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

import java.util.Map;

/**
 * Context for macro substitution.
 */
public interface MacroContext {

  /**
   * @return the logical start time of the pipeline run.
   */
  long getLogicalStartTime();

  /**
   * @return runtime arguments of the pipeline run.
   */
  Map<String, String> getRuntimeArguments();
}
