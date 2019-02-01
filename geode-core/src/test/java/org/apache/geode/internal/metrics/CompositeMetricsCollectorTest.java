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
package org.apache.geode.internal.metrics;

import static java.util.concurrent.TimeUnit.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

import org.apache.geode.metrics.MetricsCollector;

public class CompositeMetricsCollectorTest {
  private final CompositeMeterRegistry primaryRegistry = new CompositeMeterRegistry();
  private final MetricsCollector collector = new CompositeMetricsCollector(primaryRegistry);

  @Test
  public void remembersItsPrimaryRegistry() {
    CompositeMeterRegistry thePrimaryRegistry = new CompositeMeterRegistry();

    CompositeMetricsCollector collector = new CompositeMetricsCollector(thePrimaryRegistry);

    assertThat(collector.primaryRegistry())
        .isSameAs(thePrimaryRegistry);
  }

  @Test
  public void remembersAddedDownstreamRegistries() {
    MeterRegistry downstream = new SimpleMeterRegistry();

    collector.addDownstreamRegistry(downstream);

    assertThat(primaryRegistry.getRegistries())
        .contains(downstream);
  }

  @Test
  public void forgetsRemovedDownstreamRegistries() {
    MeterRegistry downstream = new SimpleMeterRegistry();
    collector.addDownstreamRegistry(downstream);

    collector.removeDownstreamRegistry(downstream);

    assertThat(primaryRegistry.getRegistries())
        .doesNotContain(downstream);
  }

  @Test
  public void defaultRegistryStartsWithNoDownstreamRegistries() {
    CompositeMetricsCollector collector = new CompositeMetricsCollector();

    MeterRegistry primaryRegistry = collector.primaryRegistry();
    assertThat(primaryRegistry)
        .isInstanceOf(CompositeMeterRegistry.class);

    Set<MeterRegistry> downstreamRegistries =
        ((CompositeMeterRegistry) primaryRegistry).getRegistries();

    assertThat(downstreamRegistries)
        .isEmpty();
  }

  @Test
  public void connectsExistingMetersToNewDownstreamRegistries() {
    MeterRegistry primaryRegistry = collector.primaryRegistry();

    String counterName = "the.counter";
    Counter primaryCounter = primaryRegistry.counter(counterName);

    double amountIncrementedBeforeConnectingDownstreamRegistry = 3.0;
    primaryCounter.increment(amountIncrementedBeforeConnectingDownstreamRegistry);

    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();
    collector.addDownstreamRegistry(downstreamRegistry);

    Counter downstreamCounter = downstreamRegistry.find(counterName).counter();
    assertThat(downstreamCounter)
        .as("downstream counter after connecting, before incrementing")
        .isNotNull();

    // Note that the newly-created downstream counter starts at zero, ignoring
    // any increments that happened before the downstream registry was added.
    assertThat(downstreamCounter.count())
        .as("downstream counter value after connecting, before incrementing")
        .isNotEqualTo(amountIncrementedBeforeConnectingDownstreamRegistry)
        .isEqualTo(0);

    double amountIncrementedAfterConnectingDownstreamRegistry = 42.0;
    primaryCounter.increment(amountIncrementedAfterConnectingDownstreamRegistry);

    assertThat(downstreamCounter.count())
        .as("downstream counter value after incrementing")
        .isEqualTo(amountIncrementedAfterConnectingDownstreamRegistry);
  }

  @Test
  public void connectsNewMetersToExistingDownstreamRegistries() {
    MeterRegistry primaryRegistry = collector.primaryRegistry();
    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();
    collector.addDownstreamRegistry(downstreamRegistry);

    String counterName = "the.counter";
    Counter newCounter = primaryRegistry.counter(counterName);

    Counter downstreamCounter = downstreamRegistry.find(counterName).counter();
    assertThat(downstreamCounter)
        .as("downstream counter before incrementing")
        .isNotNull();

    assertThat(downstreamCounter.count())
        .as("downstream counter value before incrementing")
        .isEqualTo(newCounter.count())
        .isEqualTo(0);

    double amountIncrementedAfterConnectingDownstreamRegistry = 93.0;
    newCounter.increment(amountIncrementedAfterConnectingDownstreamRegistry);

    assertThat(downstreamCounter.count())
        .as("downstream counter value after incrementing")
        .isEqualTo(newCounter.count());
  }

  @Test
  public void basicTimerWorks() {
    final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    meterRegistry.timer("myTimer","region", "users");
    Timer timerMeter= meterRegistry.get("myTimer").timer();
    timerMeter.record(10, SECONDS);
    double totalTime = meterRegistry.get("myTimer").timer().totalTime(SECONDS);
    assertThat(totalTime).isEqualTo(10);

  }

  @Test
  public void timerByTagPicksUpValues() {
    final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    final Timer timer = meterRegistry.timer("myTimer", "region", "users");
    timer.record(10, SECONDS);

    double totalTime = meterRegistry.get("myTimer")
        .tag("region","users")
        .timer()
        .totalTime(SECONDS);

    assertThat(totalTime).isEqualTo(10);
  }

  @Test
  public void timerWithDifferentTagsDoesNotAdd() {
    final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    final Timer timer = meterRegistry.timer("myTimer", "region", "users");
    final Timer timer1 = meterRegistry.timer("myTimer", "region", "customers");
    timer.record(10, SECONDS);

    double totalTime = meterRegistry.get("myTimer")
        .tag("region","customers")
        .timer()
        .totalTime(SECONDS);

    assertThat(totalTime).isEqualTo(0);
  }

  @Test
  public void timerTotalsWithDifferentTags() {
    final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    final Timer userTimer = meterRegistry.timer("myTimer", "region", "users");
    final Timer customerTimer = meterRegistry.timer("myTimer", "region", "customers");
    userTimer.record(10, SECONDS);
    customerTimer.record(20,SECONDS);

    Collection<Timer> timers = meterRegistry.get("myTimer")
        .timers();

    final Double totalTime = timers.stream()
        .mapToDouble(timer -> timer.totalTime(SECONDS))
        .sum();
    assertThat(totalTime).isEqualTo(30);

    double userTime = meterRegistry.get("myTimer")
        .tag("region","users")
        .timer()
        .totalTime(SECONDS);
    assertThat(userTime).isEqualTo(10);

    double customerTime = meterRegistry.get("myTimer")
        .tag("region","customers")
        .timer()
        .totalTime(SECONDS);
    assertThat(customerTime).isEqualTo(20);
  }

  @Test
  public void timerCountsWithDifferentTags() {
    final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    final Timer userTimer = meterRegistry.timer("myTimer", "region", "users");
    final Timer customerTimer = meterRegistry.timer("myTimer", "region", "customers");
    userTimer.record(10, SECONDS);
    customerTimer.record(20,SECONDS);
    customerTimer.record(30,SECONDS);

    Collection<Timer> timers = meterRegistry.get("myTimer")
        .timers();

    final Double totalCount = timers.stream()
        .mapToDouble(Timer::count)
        .sum();
    assertThat(totalCount).isEqualTo(3);

    double userCount = meterRegistry.get("myTimer")
        .tag("region","users")
        .timer()
        .count();
    assertThat(userCount).isEqualTo(1);

    double customerCount = meterRegistry.get("myTimer")
        .tag("region","customers")
        .timer()
        .count();
    assertThat(customerCount).isEqualTo(2);
  }

}
