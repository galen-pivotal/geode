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
package org.apache.geode.internal.statistics;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Manages Micrometer meters in a primary registry and a set of downstream registries.
 * <p>
 * The primary registry maintains the complete set of meters.
 * </p>
 * <p>
 * Adding a meter to the primary registry connects each downstream registry to the new meter.
 * </p>
 * <p>
 * Adding a downstream registry connects the downstream registry to each of the primary registry's
 * meters.
 * </p>
 * <p>
 * Removing a downstream registry disconnects the downstream registry from each of the primary
 * registry's meters.
 * </p>
 */
public interface MeterManager {
  /**
   * Returns the primary meter registry.
   *
   * @return the primary meter registry
   */
  MeterRegistry getPrimaryRegistry();

  /**
   * Adds the given registry as a "downstream" registry, connecting it to the primary registry's
   * meters.
   *
   * @param downstream the downstream registry to add
   */
  void addDownstreamRegistry(MeterRegistry downstream);

  /**
   * Removes the given downstream registry from the primary registry, disconnecting it from the
   * primary registry's meters.
   *
   * @param downstream the registry to remove as a downstream registry
   */
  void removeDownstreamRegistry(MeterRegistry downstream);
}
