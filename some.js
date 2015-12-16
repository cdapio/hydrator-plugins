/*
 * Copyright © 2015 Cask Data, Inc.
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

var basePath = __dirname + '/';
var fs = require('fs');
var path = [
  'cassandra-plugins/widgets',
  'elasticsearch-plugins/widgets',
  'hbase-plugins/widgets',
  'hdfs-plugins/widgets',
  'hive-plugins/widgets',
  'kafka-plugins/widgets',
  'mongodb-plugins/widgets',
  'python-evaluator-transform/widgets',
  'target/widgets',
  'transform-plugins/widgets'
];

path.forEach((folderName) => {
  console.log('folderName: ', basePath + folderName);
  fs.readdir(basePath + folderName, (err, files) => {
    if (err) return;
    console.log(files);
    files.forEach( (file) => {
      fs.readFile(basePath + folderName + '/' + file, 'utf-8', (err, content) => {
        if (err) throw err;
        // fs.writeFile(basePath + folderName + '/' +file, JSON.stringify(transform(content, basePath, file), null, 2), (err) => {
          // console.log('File ' + basePath + folderName + '/new-' +file + ' written successfully');
        // });
        fs.writeFile(basePath + folderName + '/' +file, JSON.stringify(changeMetadata(content, basePath, file), null, 2), (err) => {
          console.log('File ' + basePath + folderName + '/new-' +file + ' written successfully');
        });
      });
    });
  });
});

function changeMetadata(content, path, fileName) {
  console.log('filename: ', fileName, typeof content);
  var config;
  try {
    config = JSON.parse(content);
  } catch(e) {
    console.error(e);
    return;
  }
  if (!config.metadata) { return; }
  config.metadata = {
    'spec-version': '1.0'
  };
  return config;
}

function transform(content, path, fileName) {
  console.log('filename: ', fileName);
  var config;
  try {
    config = JSON.parse(content);
  } catch(e) {
    console.error(e);
    return;
  }
  if (config.metadata) { return; }
  var generatedConfig  = {
    metadata: {
      'spec-version': '1.0',
      'artifact-version': '3.3.0-SNAPSHOT',
      name: config.id || '',
      type: ''
    },
    'configuration-groups': [],
    outputs: []
  };
  if (config.groups) {
    var groups = config.groups.position;

    groups.forEach((group) => {
      var modGroup = {
        label: '',
        properties: []
      };
      var oldGroup = config.groups[group];
      var fields = oldGroup.position;
      modGroup.label = oldGroup.display;

      fields.forEach( field => {
        var oldField = oldGroup.fields[field];
        var modField = {};
        modField['widget-type'] = oldField.widget;
        modField['label'] = oldField.label || field;
        modField['name'] = field;

        if (oldField.properties) {
          modField['widget-attributes'] = oldField.properties;
        }
        modGroup.properties.push(modField);
      });
      generatedConfig['configuration-groups'].push(modGroup);
    });
  }
  if (config.outputschema) {
    if (config.outputschema.implicit) {
      generatedConfig.outputs.push({
        'widget-type': 'non-editable-schema-editor',
        'schema': config.outputschema.implicit
      });
    } else {
      var outputProperties = Object.keys(config.outputschema);
      outputProperties.forEach( property => {
        var oldSchema = config.outputschema[property];
        generatedConfig.outputs.push({
          name: property,
          label: oldSchema.label || property,
          'widget-type': oldSchema.widget,
          'widget-attributes': {
            'schema-types': oldSchema['schema-types'],
            'schema-default-type': oldSchema['schema-default-type'],
            'property-watch': oldSchema['property-watch']
          }
        })
      });
    }
  }

  return generatedConfig;
}
