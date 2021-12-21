/*
 * Copyright © 2021 Cask Data, Inc.
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
package io.cdap.plugin.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * JsonUtils contains the helper functions.
 */
public class JsonUtils {
  private static final Logger logger = Logger.getLogger(JsonUtils.class);

  public static List<KeyValue> covertJsonStringToKeyValueList(String json) {
    ObjectMapper mapper = new ObjectMapper();
    List<KeyValue> keyValueList = new ArrayList<>();
    {
      try {
        keyValueList = Arrays.asList(mapper.readValue(json, KeyValue[].class));
      } catch (IOException e) {
        logger.error("JsonUtils : Error while converting JsonString to keyValueList - " + e);
      }
    }
    return keyValueList;
  }

  public static int countJsonNodeSize(String json, String node) {
    int size = 0;
    JsonObject jsonObject = (JsonObject) new JsonParser().parse(json);
    try {
      JsonArray dataObject = jsonObject.getAsJsonArray(node);
      size = dataObject.size();
    } catch (ClassCastException e) {
      logger.error("JsonUtils : Error while converting JsonString to jsonArray - " + e);
      try {
        jsonObject.getAsJsonObject(node);
        size = 1;
      } catch (ClassCastException exception) {
        logger.error("JsonUtils : Error while converting JsonString to jsonObject - " + e);
      }
    } catch (Exception e) {
      logger.error("JsonUtils : Error while calculating json node size - " + e);
    }
    return size;
  }
}
