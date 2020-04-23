/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.common.batch;

import java.util.function.Function;

/**
 * A function that is similar to {@link Function} but allows exception being thrown from the {@link #apply(Object)}
 * method.
 *
 * @param <F> the type of the input to the function
 * @param <T> the type of the result of the function
 * @param <E> the type of the exception thrown by the function
 */
public interface ThrowableFunction<F, T, E extends Exception> {

  /**
   * Applies this function to the given argument.
   *
   * @param t the function argument
   * @return the function result
   * @throws E if application failed
   */
  T apply(F t) throws E;
}
