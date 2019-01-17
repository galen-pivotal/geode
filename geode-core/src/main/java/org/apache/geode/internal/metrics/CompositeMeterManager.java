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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

/**
 * Manages meters using a composite meter registry.
 */
public class CompositeMeterManager implements MeterManager {
  private final CompositeMeterRegistry primaryRegistry;

  /**
   * Constructs a meter manager that uses a new {@link CompositeMeterRegistry} as its primary
   * registry.
   */
  public CompositeMeterManager() {
    this(new CompositeMeterRegistry());
  }

  /**
   * Constructs a meter manager that uses the given registry as its primary registry.
   *
   * @param primaryRegistry the registry to use as the primary registry
   */
  public CompositeMeterManager(CompositeMeterRegistry primaryRegistry) {
    this.primaryRegistry = primaryRegistry;
  }

  @Override
  public MeterRegistry getPrimaryRegistry() {
    return primaryRegistry;
  }

  @Override
  public void addDownstreamRegistry(MeterRegistry downstream) {
    primaryRegistry.add(downstream);
  }

  @Override
  public void removeDownstreamRegistry(MeterRegistry downstream) {
    primaryRegistry.remove(downstream);
  }
}
