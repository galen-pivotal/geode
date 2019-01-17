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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

public class CompositeMeterManagerTest {
  private final CompositeMeterRegistry primaryRegistry = new CompositeMeterRegistry();
  private final CompositeMeterManager manager = new CompositeMeterManager(primaryRegistry);

  @Test
  public void remembersItsPrimaryRegistry() {
    CompositeMeterRegistry thePrimaryRegistry = new CompositeMeterRegistry();

    CompositeMeterManager manager = new CompositeMeterManager(thePrimaryRegistry);

    assertThat(manager.getPrimaryRegistry())
        .isSameAs(thePrimaryRegistry);
  }

  @Test
  public void connectsDownstreamRegistry() {
    MeterRegistry downstream = new SimpleMeterRegistry();

    manager.addDownstreamRegistry(downstream);

    assertThat(primaryRegistry.getRegistries())
        .contains(downstream);
  }

  @Test
  public void removesDownstreamRegistry() {
    MeterRegistry downstream = new SimpleMeterRegistry();
    manager.addDownstreamRegistry(downstream);

    manager.removeDownstreamRegistry(downstream);

    assertThat(primaryRegistry.getRegistries())
        .doesNotContain(downstream);
  }

  @Test
  public void defaultRegistryStartsWithNoDownstreamRegistries() {
    CompositeMeterManager compositeMeterManager = new CompositeMeterManager();

    MeterRegistry primaryRegistry = compositeMeterManager.getPrimaryRegistry();
    assertThat(primaryRegistry)
        .isInstanceOf(CompositeMeterRegistry.class);

    Set<MeterRegistry> downstreamRegistries =
        ((CompositeMeterRegistry) primaryRegistry).getRegistries();
    assertThat(downstreamRegistries)
        .isEmpty();
  }
}
