/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.format;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Collects all metadata fields available to retrieve.
 */
public enum MetadataField {
    FILE_LENGTH("path.tracking.length.field", Schema.Type.LONG),
    FILE_MODIFICATION_TIME("path.tracking.mod.field", Schema.Type.LONG);

    private final String confName;
    private final Schema.Type schemaType;

    public String getConfName() {
        return confName;
    }

    public Schema.Type getSchemaType() {
        return schemaType;
    }

    MetadataField(String confName, Schema.Type schemaType) {
        this.confName = confName;
        this.schemaType = schemaType;
    }

    public static MetadataField getMetadataField(String fieldName) {
        for (MetadataField metadataField : MetadataField.values()) {
            if (metadataField.name().equals(fieldName)) {
                return metadataField;
            }
        }
        throw new IllegalArgumentException("Invalid metadata field name: " + fieldName);
    }
}
