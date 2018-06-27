/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.api;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.api.AsyncRegion;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.UnitTest;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class AsyncRegionImplJUnitTest {
  public static final String TEST_VALUE = "testValue";
  public static final String TEST_KEY = "testKey";
  private Region<String, String> region;
  private AsyncRegion<String, String> asyncRegion;

  @Before
  public void setUp() {
    region = mock(Region.class);
    asyncRegion = new AsyncRegionImpl<>(region, mock(InternalCache.class));
  }

  @Test
  public void GetCompletableFuture() throws Exception {
    doReturn(TEST_VALUE).when(region).get(TEST_KEY);

    CompletableFuture<String> getFuture = asyncRegion.get(TEST_KEY);

    String returnedValue = getFuture.get();

    Assert.assertEquals(TEST_VALUE, returnedValue);
  }

  @Test
  public void PutCompletableFuture() throws Exception {
    doReturn(TEST_VALUE).when(region).put(TEST_KEY,"value2");

    CompletableFuture<String> putFuture = asyncRegion.put(TEST_KEY,"value2");

    String returnedValue = putFuture.get();

    Assert.assertEquals(TEST_VALUE, returnedValue);
  }
}
