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

package co.cask.hydrator.plugin;

import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupTableConfig;

import java.util.Set;

/**
* Provides lookup functions for {@link ValueMapper}.
* Used directly by {@link ValueMapper}.
 */
public class ValueMapperLookUp {
  private final Lookup<Object> delegate;
  private final LookupTableConfig config;

  public ValueMapperLookUp(Lookup<Object> delegate, LookupTableConfig config) {
    this.config = config;
    this.delegate = delegate;
  }

  public Object lookup(String key) {
    return delegate.lookup(key);
  }

  public Object lookup(String... keys) {
    return delegate.lookup(keys);
  }

  public Object lookup(Set<String> keys) {
    return delegate.lookup(keys);
  }
}

