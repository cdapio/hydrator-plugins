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

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for parsing and validation done by {@link GroupByAggregator}.
 */
public class GroupByAggregatorConfigTest {

  @Test
  public void testParsing() {
    GroupByConfig config = new GroupByConfig(" user ,item, email ",
                                             " avgPrice : avg(price) , " +
                                               "numPurchases :  count( *  )," +
                                               "numCoupons:count(coupon)," +
                                               "totalSpent:sum( price), " +
                                               "firstItem:first(item ) ," +
                                               "lastItem: last( item)," +
                                               "smallestPurchase:min( price) , " +
                                               "largestPurchase :max(price ) ," +
                                               "itemList :CollectList(item) ," +
                                               "itemSet :CollectSet(item)");
    Assert.assertEquals(ImmutableList.of("user", "item", "email"), config.getGroupByFields());
    List<GroupByConfig.FunctionInfo> expected = ImmutableList.of(
      new GroupByConfig.FunctionInfo("avgPrice", "price", GroupByConfig.Function.AVG),
      new GroupByConfig.FunctionInfo("numPurchases", "*", GroupByConfig.Function.COUNT),
      new GroupByConfig.FunctionInfo("numCoupons", "coupon", GroupByConfig.Function.COUNT),
      new GroupByConfig.FunctionInfo("totalSpent", "price", GroupByConfig.Function.SUM),
      new GroupByConfig.FunctionInfo("firstItem", "item", GroupByConfig.Function.FIRST),
      new GroupByConfig.FunctionInfo("lastItem", "item", GroupByConfig.Function.LAST),
      new GroupByConfig.FunctionInfo("smallestPurchase", "price", GroupByConfig.Function.MIN),
      new GroupByConfig.FunctionInfo("largestPurchase", "price", GroupByConfig.Function.MAX),
      new GroupByConfig.FunctionInfo("itemList", "item", GroupByConfig.Function.COLLECTLIST),
      new GroupByConfig.FunctionInfo("itemSet", "item", GroupByConfig.Function.COLLECTSET)
    );
    Assert.assertEquals(expected, config.getAggregates());
  }
}
