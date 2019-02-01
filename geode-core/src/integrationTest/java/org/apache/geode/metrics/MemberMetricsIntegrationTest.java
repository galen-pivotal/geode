/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 *  * agreements. See the NOTICE file distributed with this work for additional information regarding
 *  * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance with the License. You may obtain a
 *  * copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the License
 *  * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  * or implied. See the License for the specific language governing permissions and limitations under
 *  * the License.
 *
 */

package org.apache.geode.metrics;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

public class MemberMetricsIntegrationTest {

  private Cache cache;
  private Region<Object, Object> testRegion;

  @Before
  public void setup() {
    final CacheFactory cacheFactory = new CacheFactory();
    DSy
    cacheFactory.create(mock(InternalDistributedSystem.class))
    this.cache = cacheFactory.create();

  }

  @After
  public void teardown() {
    cache.close();
  }

  @Test
  public void registersGCMeter() {
    final MetricsCollector metricsCollector = cache.getDistributedSystem().getMetricsCollector();
    final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    metricsCollector.addDownstreamRegistry(meterRegistry);
    testRegion = cache.createRegionFactory().create("testRegion");



    assertThat(meterRegistry.get("regionGets").timer().count())
        .as("gets before any operations")
        .isEqualTo(0);

    testRegion.get("testKey");

    final double
        regionGets =
        meterRegistry.get("regionGets").timer().count();

    assertThat(regionGets).isEqualTo(1);
  }

  @Test
  public void totalTimeIsCorrect() {
    final Cache cache = new CacheFactory().create();
    final MetricsCollector metricsCollector = cache.getDistributedSystem().getMetricsCollector();
    Clock testClock = new Clock() {
      final AtomicLong count = new AtomicLong(0);

      @Override
      public long wallTime() {
        return count.incrementAndGet();
      }

      @Override
      public long monotonicTime() {
        return count.incrementAndGet();
      }
    };

    final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, testClock);

    metricsCollector.addDownstreamRegistry(meterRegistry);
    testRegion = cache.createRegionFactory().create("testRegion");


    assertThat(meterRegistry.get("regionGets").timer().totalTime(TimeUnit.NANOSECONDS))
        .as("gets time before any operations")
        .isEqualTo(0);

    testRegion.get("testKey");

    assertThat(meterRegistry.get("regionGets").timer().totalTime(TimeUnit.NANOSECONDS))
        .as("gets time after any operations")
        .isEqualTo(1);
  }

  @Ignore
  @Test
  public void meterIsRemovedWhenRegionIsDestroyed() {

  }

  @Ignore
  @Test
  public void multipleRegionMetricsWorkTogetherProperly() {

  }
}