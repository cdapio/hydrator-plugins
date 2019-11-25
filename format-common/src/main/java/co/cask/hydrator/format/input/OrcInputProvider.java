/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.input;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.format.OrcToStructuredTransformer;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides Orc formatters.
 */
public class OrcInputProvider implements FileInputFormatterProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OrcInputProvider.class);

    @Nullable
    @Override
    public Schema getSchema(@Nullable String pathField) {
        Configuration configuration = new Configuration();
        if (Strings.isNullOrEmpty(pathField)) {
            throw new IllegalArgumentException("Path is a required field for fetching Schema");
        }
        Path path = new Path(pathField);
        try {
            Reader reader = OrcFile.createReader(path, new OrcFile.ReaderOptions(configuration));
            TypeDescription typeDescription = reader.getSchema();
            return OrcToStructuredTransformer.convertSchema(typeDescription);
        } catch (Exception e) {
            LOG.error("Error in fetching orc schema => " + e.getMessage(), e);
            throw new RuntimeException("Error in fetching orc schema => " + e.getMessage(), e);
        }
    }

    @Override
    public FileInputFormatter create(Map<String, String> properties, @Nullable Schema schema) {
        return new OrcInputFormatter(schema);
    }
}
