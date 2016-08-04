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

package co.cask.hydrator.plugin.batch.aggregator;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit test for {@link CompositeKey} classes.
 */
public class CompositeKeySortingTest {

  private static final Schema ADDRESS = Schema.recordOf("address",
                                                        Schema.Field.of("city", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("state", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("zip", Schema.of(Schema.Type.LONG)));

  MapReduceDriver<LongWritable, Text, CompositeKey, Text, CompositeKey, Text> mapReduceDriver;
  MapDriver<LongWritable, Text, CompositeKey, Text> mapDriver;
  ReduceDriver<CompositeKey, Text, CompositeKey, Text> reduceDriver;

  @Before
  public void setUp() throws InterruptedException, IOException {
    CompositeKeyMapper mapper = new CompositeKeyMapper();
    CompositeKeyReducer reducer = new CompositeKeyReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    mapReduceDriver.setKeyOrderComparator(new CompositeKeyComparator());
    mapReduceDriver.setKeyGroupingComparator(new CompositeKeyGroupComparator());
  }

  @Test
  public void testSortingForSimpleRecord() throws IOException {
    Gson gson = new GsonBuilder().create();
    Schema input =
      Schema.recordOf("input", Schema.Field.of("productName", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("quantity", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
    StructuredRecord structuredRecord1 = StructuredRecord.builder(input)
      .set("productName", "Angels & Demons Hardback")
      .set("quantity", 1)
      .set("price", 20.5)
      .build();

    StructuredRecord structuredRecord2 = StructuredRecord.builder(input)
      .set("productName", "Angels & Demons Hardback")
      .set("quantity", 2)
      .set("price", 20.5)
      .build();

    StructuredRecord structuredRecord3 = StructuredRecord.builder(input)
      .set("productName", "Angels & Demons Hardback")
      .set("quantity", 2)
      .set("price", 25.2)
      .build();

    StructuredRecord structuredRecord4 = StructuredRecord.builder(input)
      .set("productName", "Angels & Demons Hardback")
      .set("quantity", null)
      .set("price", 20.5)
      .build();

    StructuredRecord structuredRecord5 = StructuredRecord.builder(input)
      .set("productName", "Inferno HardBack")
      .set("quantity", 1)
      .set("price", 30)
      .build();

    Type schemaType = new TypeToken<Schema>() { }.getType();
    Type mapType = new TypeToken<LinkedHashMap>() { }.getType();

    Configuration conf = mapReduceDriver.getConfiguration();
    Map<String, String> map = new LinkedHashMap<>();
    map.put("productName", "asc");
    map.put("price", "desc");
    map.put("quantity", "desc");
    conf.set("sortFieldList", gson.toJson(map, mapType));
    conf.set("schema", gson.toJson(input, schemaType));

    mapReduceDriver.withInput(new LongWritable(), new Text(StructuredRecordStringConverter
                                                             .toJsonString(structuredRecord1)));
    mapReduceDriver.withInput(new LongWritable(), new Text(StructuredRecordStringConverter
                                                             .toJsonString(structuredRecord2)));
    mapReduceDriver.withInput(new LongWritable(), new Text(StructuredRecordStringConverter
                                                             .toJsonString(structuredRecord3)));
    mapReduceDriver.withInput(new LongWritable(), new Text(StructuredRecordStringConverter
                                                             .toJsonString(structuredRecord4)));
    mapReduceDriver.withInput(new LongWritable(), new Text(StructuredRecordStringConverter
                                                             .toJsonString(structuredRecord5)));

    List<Pair<CompositeKey, Text>> output = mapReduceDriver.run();

    StructuredRecord record1 = StructuredRecordStringConverter.fromJsonString(output.get(0).getSecond().toString(),
                                                                              input);
    Assert.assertEquals("Angels & Demons Hardback", record1.get("productName"));
    Assert.assertEquals(25.2, (Double) record1.get("price"), 0.0);
    Assert.assertEquals(2, record1.get("quantity"));

    StructuredRecord record2 = StructuredRecordStringConverter.fromJsonString(output.get(2).getSecond().toString(),
                                                                              input);
    Assert.assertEquals("Angels & Demons Hardback", record1.get("productName"));
    Assert.assertEquals(20.5, (Double) record2.get("price"), 0.0);
    Assert.assertEquals(1, record2.get("quantity"));

    StructuredRecord record3 = StructuredRecordStringConverter.fromJsonString(output.get(4).getSecond().toString(),
                                                                              input);
    Assert.assertEquals("Inferno HardBack", record3.get("productName"));
    Assert.assertEquals(30.0, (Double) record3.get("price"), 0.0);
    Assert.assertEquals(1, record3.get("quantity"));

    StructuredRecord record4 = StructuredRecordStringConverter.fromJsonString(output.get(3).getSecond().toString(),
                                                                              input);
    Assert.assertEquals("Angels & Demons Hardback", record4.get("productName"));
    Assert.assertEquals(20.5, (Double) record4.get("price"), 0.0);
    Assert.assertEquals(null, record4.get("quantity"));
  }

  @Test
  public void testSortingForNestedRecords() throws IOException {
    Gson gson = new GsonBuilder().create();
    Schema input =
      Schema.recordOf("input", Schema.Field.of("productName", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("quantity", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                      Schema.Field.of("shippingAddress",
                                      Schema.recordOf("address", Schema.Field.of("city", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("state", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("zip", Schema.of(Schema.Type.LONG)))),
                      Schema.Field.of("billingAddress",
                                      Schema.recordOf("address", Schema.Field.of("city", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("state", Schema.of(Schema.Type.STRING)),
                                                      Schema.Field.of("zip", Schema.of(Schema.Type.LONG)))));
    StructuredRecord structuredRecord1 = StructuredRecord.builder(input)
      .set("productName", "Angels & Demons Hardback")
      .set("quantity", 1)
      .set("price", 20.5)
      .set("shippingAddress", getAddress("Douglas", "Arizona", 11120))
      .set("billingAddress", getAddress("Douglas", "Arizona", 11120))
      .build();

    StructuredRecord structuredRecord2 = StructuredRecord.builder(input)
      .set("productName", "Angels & Demons Hardback")
      .set("quantity", 2)
      .set("price", 20.5)
      .set("shippingAddress", getAddress("Douglas", "Arizona", 11120))
      .set("billingAddress", getAddress("Belmont", "California", 11210))
      .build();

    StructuredRecord structuredRecord3 = StructuredRecord.builder(input)
      .set("productName", "Angels & Demons Hardback")
      .set("quantity", 2)
      .set("price", 25.2)
      .set("shippingAddress", getAddress("California City", "California", 11220))
      .set("billingAddress", getAddress("Columbia", "Illinois", 12568))
      .build();

    StructuredRecord structuredRecord4 = StructuredRecord.builder(input)
      .set("productName", "Angels & Demons Hardback")
      .set("quantity", 1)
      .set("price", 20.5)
      .set("shippingAddress", getAddress("California City", "California", 11220))
      .set("billingAddress", getAddress("Wichita", "Kansas", 16485))
      .build();

    StructuredRecord structuredRecord5 = StructuredRecord.builder(input)
      .set("productName", "Inferno HardBack")
      .set("quantity", 1)
      .set("price", 30)
      .set("shippingAddress", getAddress("Chenoa", "Illinois", 12575))
      .set("billingAddress", getAddress("Wichita", "Kansas", 16481))
      .build();

    Type schemaType = new TypeToken<Schema>() { }.getType();
    Type mapType = new TypeToken<LinkedHashMap>() { }.getType();

    Configuration conf = mapReduceDriver.getConfiguration();
    Map<String, String> map = new LinkedHashMap<>();
    map.put("productName", "asc");
    map.put("price", "desc");
    map.put("shippingAddress:city", "asc");
    map.put("billingAddress:zip", "desc");
    conf.set("sortFieldList", gson.toJson(map, mapType));
    conf.set("schema", gson.toJson(input, schemaType));

    mapReduceDriver.withInput(new LongWritable(), new Text(StructuredRecordStringConverter
                                                             .toJsonString(structuredRecord1)));
    mapReduceDriver.withInput(new LongWritable(), new Text(StructuredRecordStringConverter
                                                             .toJsonString(structuredRecord2)));
    mapReduceDriver.withInput(new LongWritable(), new Text(StructuredRecordStringConverter
                                                             .toJsonString(structuredRecord3)));
    mapReduceDriver.withInput(new LongWritable(), new Text(StructuredRecordStringConverter
                                                             .toJsonString(structuredRecord4)));
    mapReduceDriver.withInput(new LongWritable(), new Text(StructuredRecordStringConverter
                                                             .toJsonString(structuredRecord5)));

    List<Pair<CompositeKey, Text>> output = mapReduceDriver.run();

    StructuredRecord record1 = StructuredRecordStringConverter.fromJsonString(output.get(0).getSecond().toString(),
                                                                              input);
    Assert.assertEquals("Angels & Demons Hardback", record1.get("productName"));
    Assert.assertEquals(25.2, (Double) record1.get("price"), 0.0);
    StructuredRecord shippingRecord1 = record1.get("shippingAddress");
    StructuredRecord billingRecord1 = record1.get("billingAddress");

    Assert.assertEquals("California City", shippingRecord1.get("city"));
    Assert.assertEquals(11220L, shippingRecord1.get("zip"));
    Assert.assertEquals("Columbia", billingRecord1.get("city"));
    Assert.assertEquals(12568L, billingRecord1.get("zip"));

    StructuredRecord record2 = StructuredRecordStringConverter.fromJsonString(output.get(2).getSecond().toString(),
                                                                              input);
    Assert.assertEquals("Angels & Demons Hardback", record1.get("productName"));
    Assert.assertEquals(20.5, (Double) record2.get("price"), 0.0);
    StructuredRecord shippingRecord2 = record2.get("shippingAddress");
    StructuredRecord billingRecord2 = record2.get("billingAddress");

    Assert.assertEquals("Douglas", shippingRecord2.get("city"));
    Assert.assertEquals(11120L, shippingRecord2.get("zip"));
    Assert.assertEquals("Belmont", billingRecord2.get("city"));
    Assert.assertEquals(11210L, billingRecord2.get("zip"));


    StructuredRecord record3 = StructuredRecordStringConverter.fromJsonString(output.get(4).getSecond().toString(),
                                                                              input);
    Assert.assertEquals("Inferno HardBack", record3.get("productName"));
    Assert.assertEquals(30.0, (Double) record3.get("price"), 0.0);
    StructuredRecord shippingRecord3 = record3.get("shippingAddress");
    StructuredRecord billingRecord3 = record3.get("billingAddress");

    Assert.assertEquals("Chenoa", shippingRecord3.get("city"));
    Assert.assertEquals(12575L, shippingRecord3.get("zip"));
    Assert.assertEquals("Wichita", billingRecord3.get("city"));
    Assert.assertEquals(16481L, billingRecord3.get("zip"));
  }

  private StructuredRecord getAddress(String city, String state, int zip) {
    return StructuredRecord.builder(ADDRESS)
      .set("city", city)
      .set("state", state)
      .set("zip", zip)
      .build();
  }
}
