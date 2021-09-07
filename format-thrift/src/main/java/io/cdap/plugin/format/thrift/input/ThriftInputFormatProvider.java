/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.format.thrift.input;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.format.input.PathTrackingConfig;
import io.cdap.plugin.format.input.PathTrackingInputFormatProvider;
import java.io.IOException;

/**
 * Input reading logic for Thrift files.
 */
@Plugin(type = ValidatingInputFormat.PLUGIN_TYPE)
@Name(ThriftInputFormatProvider.NAME)
@Description(ThriftInputFormatProvider.DESC)
public class ThriftInputFormatProvider extends PathTrackingInputFormatProvider<ThriftInputFormatProvider.ThriftConfig> {
    static final String NAME = "thrift";
    static final String DESC = "Plugin for reading files in thrift format.";
    public static final PluginClass PLUGIN_CLASS =
        new PluginClass(ValidatingInputFormat.PLUGIN_TYPE, NAME, DESC, ThriftInputFormatProvider.class.getName(),
            "conf", PathTrackingConfig.FIELDS);
    // this has to be here to be able to instantiate the correct plugin property field
    private final ThriftConfig conf;

    public ThriftInputFormatProvider(ThriftConfig conf) {
        super(conf);
        this.conf = conf;
    }

    @Override
    public String getInputFormatClassName() {
        return CombineThriftInputFormat.class.getName();
    }

    public static class ThriftConfig extends PathTrackingConfig {

        /**
         * Return the configured schema, or the default schema if none was given. Should never be called if the
         * schema contains a macro
         */
        @Override
        public Schema getSchema() {
            if (containsMacro(NAME_SCHEMA)) {
                return null;
            }
            if (Strings.isNullOrEmpty(schema)) {
                return getDefaultSchema();
            }
            try {
                return Schema.parseJson(schema);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
            }
        }

        private Schema getDefaultSchema() {
            return null;
        }
    }
}
