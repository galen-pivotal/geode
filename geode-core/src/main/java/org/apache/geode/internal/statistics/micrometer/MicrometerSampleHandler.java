/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.statistics.micrometer;

import java.util.List;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.logging.log4j.Logger;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.statistics.ResourceInstance;
import org.apache.geode.internal.statistics.ResourceType;
import org.apache.geode.internal.statistics.SampleHandler;
import org.apache.geode.internal.statistics.StatisticDescriptorImpl;

public class MicrometerSampleHandler implements SampleHandler {

  private static final Logger logger = LogService.getLogger();

  private final MeterRegistry registry = new SimpleMeterRegistry();

  public MicrometerSampleHandler() {
    logger.info("MicrometerSampleHandler ctor");
  }

  @Override
  public void sampled(long nanosTimeStamp, List<ResourceInstance> resourceInstances) {
    for (ResourceInstance resourceInstance : resourceInstances) {
      updateMeters(resourceInstance);
    }
  }

  @Override
  public void allocatedResourceType(ResourceType resourceType) {
    logger.info("MicrometerSampleHandler: allocatedResourceType {}", resourceType);
  }

  @Override
  public void allocatedResourceInstance(ResourceInstance resourceInstance) {
    logger.info("MicrometerSampleHandler: allocatedResourceInstance {}", resourceInstance);
    ResourceType resourceType = resourceInstance.getResourceType();
    for (StatisticDescriptor descriptor : resourceType.getStatisticDescriptors()) {
      String name = getMeterName(resourceInstance, descriptor);
      Counter counter = Counter.builder(name)
          .description(descriptor.getDescription())
          .baseUnit(descriptor.getUnit())
          .tag("name", resourceInstance.getStatistics().getTextId())
          .register(registry);
      Meter.Id id = counter.getId();
      logger.info("MicrometerSampleHandler: created counter {}, {}, {}, {} for resource {}", id,
          id.getDescription(), id.getBaseUnit(), id.getType(), descriptor);
    }
  }

  @Override
  public void destroyedResourceInstance(ResourceInstance resourceInstance) {
    // nothing
  }

  public MeterRegistry getMeterRegistry() {
    return registry;
  }

  private void updateMeters(ResourceInstance ri) {
    if (ri.getStatistics().isClosed()) {
      return;
    }

    StatisticDescriptor[] descriptors = ri.getResourceType().getStatisticDescriptors();
    int[] updatedStats = ri.getUpdatedStats();

    for (int i = 0; i < updatedStats.length; i++) {
      int descriptorIndex = updatedStats[i];
      StatisticDescriptor descriptor = descriptors[descriptorIndex];
      StatisticDescriptorImpl descriptorImpl = (StatisticDescriptorImpl) descriptor;
      long rawbits = ri.getLatestStatValues()[descriptorIndex];
      Number value = descriptorImpl.getNumberForRawBits(rawbits);

      updateMeter(ri, descriptor, value);
    }
  }

  private void updateMeter(ResourceInstance ri, StatisticDescriptor stat, Number value) {
    // TODO: need to change from Counter to Supplier

    Search search = registry.find(getMeterName(ri, stat));
    Counter counter = search.counter();

    counter.increment();
  }

  private String getMeterName(ResourceInstance ri, StatisticDescriptor stat) {
    return ri.getResourceType().getStatisticsType().getName() + "." + stat.getName();
  }
}
