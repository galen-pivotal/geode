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
package org.apache.geode.internal.statistics.micrometer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.statistics.LocalStatisticsImpl;
import org.apache.geode.internal.statistics.StatisticsImpl;
import org.apache.geode.internal.statistics.StatisticsRegistry;

/**
 * A statistics registry that maintains a Micrometer meter for each statistics value.
 */
public class MeteredStatisticsRegistry extends StatisticsRegistry {
  private final List<Statistics> statisticsInstances = new CopyOnWriteArrayList<>();
  private final MeterRegistrar meterRegistrar;
  private int statisticsListModCount = 0;

  public MeteredStatisticsRegistry(String systemName, long startTime,
      MeterRegistrar meterRegistrar) {
    super(systemName, startTime);
    this.meterRegistrar = meterRegistrar;
  }

  @Override
  public List<Statistics> getStatsList() {
    return statisticsInstances;
  }

  @Override
  public int getStatListModCount() {
    return statisticsListModCount;
  }

  @Override
  public void destroyStatistics(Statistics statistics) {
    deregisterStatistics(statistics);
  }

  @Override
  protected Statistics newAtomicStatistics(StatisticsType type, long uniqueId, long numericId,
      String textId) {
    Statistics atomicStatistics =
        StatisticsImpl.createAtomicNoOS(type, textId, numericId, uniqueId, this);
    registerStatistics(atomicStatistics);
    return atomicStatistics;
  }

  @Override
  protected LocalStatisticsImpl newOsStatistics(StatisticsType type, long uniqueId, long numericId,
      String textId, int osStatFlags) {
    LocalStatisticsImpl localStatistics =
        new LocalStatisticsImpl(type, textId, numericId, uniqueId, false, osStatFlags, this);
    registerStatistics(localStatistics);
    return localStatistics;
  }

  private void registerStatistics(Statistics statistics) {
    synchronized (statisticsInstances) {
      statisticsInstances.add(statistics);
      meterRegistrar.registerStatistics(statistics);
      statisticsListModCount++;
    }
  }

  private void deregisterStatistics(Statistics statistics) {
    synchronized (statisticsInstances) {
      if (statisticsInstances.remove(statistics)) {
        meterRegistrar.deregisterStatistics(statistics);
        statisticsListModCount++;
      }
    }
  }
}
