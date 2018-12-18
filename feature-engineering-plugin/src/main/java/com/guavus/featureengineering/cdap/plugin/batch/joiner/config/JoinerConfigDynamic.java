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

package com.guavus.featureengineering.cdap.plugin.batch.joiner.config;

import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;

/**
 * Config for join plugin.
 */
public class JoinerConfigDynamic extends JoinerConfig {
    /**
     * 
     */
    private static final long serialVersionUID = 7452282169333852117L;

    public JoinerConfigDynamic() {
        super();
    }

    @VisibleForTesting
    JoinerConfigDynamic(String joinKeys, String selectedFields, String requiredInputs, String keysToBeAppended) {
        super(joinKeys, selectedFields, requiredInputs, keysToBeAppended);
    }

    @Override
    public Map<String, String> getKeysToBeAppended() {
        Map<String, String> keysToBeAppendedMap = new HashMap<>();
        if (this.keysToBeAppended != null && !this.keysToBeAppended.isEmpty() && !this.keysToBeAppended.equals("NA")) {
            String tokens[] = keysToBeAppended.split(",");
            for (String token : tokens) {
                if (token.trim().isEmpty()) {
                    continue;
                }
                String[] values = token.trim().split("\\.");
                keysToBeAppendedMap.put(values[0].trim(), values[1].trim());
            }
        }
        return keysToBeAppendedMap;
    }

    @Override
    public Boolean getIsDynamicSchema() {
        return true;
    }
}
