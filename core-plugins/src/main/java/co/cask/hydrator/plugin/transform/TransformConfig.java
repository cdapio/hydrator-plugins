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

package co.cask.hydrator.plugin.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Base aggregator config, since almost all aggregators should support setting
 * the number of partitions.
 */
public class TransformConfig extends PluginConfig {
    /**
     * 
     */
    private static final long serialVersionUID = 9178561772366181317L;

    @Nullable
    @Description("Number of partitions to use when transforming. If not specified, the execution framework "
            + "will decide how many to use.")
    public Integer numPartitions;
    
    @Nullable
    @Description("Flag to set if schema is static or dynamic.")
    public Boolean isDynamicSchema = false;

}
