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

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;

public class StatisticsRegistry implements StatisticsManager {
  private static final StatisticsTypeFactory statisticsTypeFactory =
      StatisticsTypeFactoryImpl.singleton();
  private final List<Statistics> statisticsInstances = new CopyOnWriteArrayList<>();
  private final AtomicLong statisticsListUniqueId = new AtomicLong(1);
  private final String systemName;
  private final long startTime;
  private int statisticsListModCount = 0;

  public StatisticsRegistry(String systemName, long startTime) {
    this.systemName = systemName;
    this.startTime = startTime;
  }

  @Override
  public String getName() {
    return systemName;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public Statistics[] getStatistics() {
    return statisticsInstances.toArray(new Statistics[0]);
  }

  @Override
  public List<Statistics> getStatsList() {
    return statisticsInstances;
  }

  @Override
  public int getStatisticsCount() {
    return statisticsInstances.size();
  }

  @Override
  public int getStatListModCount() {
    return statisticsListModCount;
  }

  @Override
  public StatisticsType createType(String name, String description, StatisticDescriptor[] stats) {
    return statisticsTypeFactory.createType(name, description, stats);
  }

  @Override
  public StatisticsType[] createTypesFromXml(Reader reader) throws IOException {
    return statisticsTypeFactory.createTypesFromXml(reader);
  }

  @Override
  public StatisticsType findType(String name) {
    return statisticsTypeFactory.findType(name);
  }

  @Override
  public Statistics createStatistics(StatisticsType type) {
    return createOsStatistics(type, null, 0, 0);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type) {
    return createAtomicStatistics(type, null, 0);
  }

  @Override
  public Statistics createStatistics(StatisticsType type, String textId) {
    return createOsStatistics(type, textId, 0, 0);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId) {
    return createAtomicStatistics(type, textId, 0);
  }

  @Override
  public Statistics createStatistics(StatisticsType type, String textId, long numericId) {
    return createOsStatistics(type, textId, numericId, 0);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId) {
    long uniqueId = statisticsListUniqueId.getAndIncrement();
    Statistics atomicStatistics =
        StatisticsImpl.createAtomicNoOS(type, textId, numericId, uniqueId, this);
    registerStatistics(atomicStatistics);
    return atomicStatistics;
  }

  @Override
  public Statistics createOsStatistics(StatisticsType type, String textId, long numericId,
      int osStatFlags) {
    long uniqueId = statisticsListUniqueId.getAndIncrement();
    Statistics osStatistics =
        new LocalStatisticsImpl(type, textId, numericId, uniqueId, false, osStatFlags, this);
    registerStatistics(osStatistics);
    return osStatistics;
  }

  @Override
  public Statistics findStatistics(long uniqueId) {
    return anyStatisticsInstance(withUniqueId(uniqueId))
        .orElseThrow(() -> new RuntimeException(
            "Could not find statistics instance with unique id " + uniqueId));
  }

  @Override
  public boolean statisticsExists(long uniqueId) {
    return anyStatisticsInstance(withUniqueId(uniqueId))
        .isPresent();
  }

  public Statistics findStatisticsByUniqueId(long uniqueId) {
    return anyStatisticsInstance(withUniqueId(uniqueId))
        .orElse(null);
  }

  @Override
  public Statistics[] findStatisticsByNumericId(long numericId) {
    return allStatisticsInstances(withNumericId(numericId))
        .toArray(Statistics[]::new);
  }

  @Override
  public Statistics[] findStatisticsByTextId(String textId) {
    return allStatisticsInstances(withTextId(textId))
        .toArray(Statistics[]::new);
  }

  @Override
  public Statistics[] findStatisticsByType(StatisticsType type) {
    return allStatisticsInstances(withStatisticsType(type))
        .toArray(Statistics[]::new);
  }

  @Override
  public void destroyStatistics(Statistics stats) {
    deregisterStatistics(stats);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units) {
    return statisticsTypeFactory.createIntCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units) {
    return statisticsTypeFactory.createLongCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units) {
    return statisticsTypeFactory.createDoubleCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units) {
    return statisticsTypeFactory.createIntGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units) {
    return statisticsTypeFactory.createLongGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units) {
    return statisticsTypeFactory.createDoubleGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units,
      boolean largerBetter) {
    return statisticsTypeFactory.createIntCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units,
      boolean largerBetter) {
    return statisticsTypeFactory.createLongCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units,
      boolean largerBetter) {
    return statisticsTypeFactory.createDoubleCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units,
      boolean largerBetter) {
    return statisticsTypeFactory.createIntGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units,
      boolean largerBetter) {
    return statisticsTypeFactory.createLongGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units,
      boolean largerBetter) {
    return statisticsTypeFactory.createDoubleGauge(name, description, units, largerBetter);
  }

  private void registerStatistics(Statistics instance) {
    synchronized (statisticsInstances) {
      statisticsInstances.add(instance);
      statisticsListModCount++;
    }
  }

  private void deregisterStatistics(Statistics instance) {
    synchronized (statisticsInstances) {
      if (statisticsInstances.remove(instance)) {
        statisticsListModCount++;
      }
    }
  }

  private Stream<Statistics> allStatisticsInstances(Predicate<? super Statistics> predicate) {
    return statisticsInstances.stream().filter(predicate);
  }

  private Optional<Statistics> anyStatisticsInstance(Predicate<? super Statistics> predicate) {
    return allStatisticsInstances(predicate).findAny();
  }

  private static Predicate<Statistics> withNumericId(long numericId) {
    return statistics -> statistics.getNumericId() == numericId;
  }

  private static Predicate<Statistics> withStatisticsType(StatisticsType type) {
    return statistics -> statistics.getType() == type;
  }

  private static Predicate<Statistics> withTextId(String textId) {
    return statistics -> textId.equals(statistics.getTextId());
  }

  private static Predicate<Statistics> withUniqueId(long uniqueId) {
    return statistics -> statistics.getUniqueId() == uniqueId;
  }
}
