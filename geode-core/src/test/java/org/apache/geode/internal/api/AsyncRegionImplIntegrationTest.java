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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.api.AsyncRegion;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(IntegrationTest.class)
public class AsyncRegionImplIntegrationTest {
  public static final String TEST_KEY = "testKey";
  public static final String TEST_REGION = "testRegion";
  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule().withAutoStart();

  @Test
  public void get() throws Exception {
    final InternalCache cache = serverStarterRule.getCache();
    final RegionFactory<String, Object> regionFactory = cache.createRegionFactory();
    final AsyncRegion<String, Object> testRegion =
        regionFactory.createAsyncRegion(TEST_REGION);

    CompletableFuture<Object> putFuture = testRegion.put(TEST_KEY, new TestSerializable("testValue"));
    putFuture.get();

    CompletableFuture<PdxInstance> pdxInstanceFuture = testRegion.getPdx(TEST_KEY);
    final PdxInstance returnedPdxInstance = pdxInstanceFuture.get();

    assertEquals(TestSerializable.class.getName(), returnedPdxInstance.getClassName());

    final TestSerializable actual = (TestSerializable) returnedPdxInstance.getObject();
    assertEquals("testValue", actual.getValue());
  }

  @Test
  public void putAllDoWorkGet() throws Exception {

  }

  private static class TestSerializable implements PdxSerializable {
    public String getValue() {
      return value;
    }

    private String value;

    public TestSerializable() {}

    public TestSerializable(String value) {
      this.value = value;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("testField", value);
    }

    @Override
    public void fromData(PdxReader reader) {
      value = reader.readString("testField");
    }
  }
}