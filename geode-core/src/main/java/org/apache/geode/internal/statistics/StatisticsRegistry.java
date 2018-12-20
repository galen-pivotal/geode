package org.apache.geode.internal.statistics;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.distributed.internal.DistributionConfig;

public class StatisticsRegistry implements StatisticsManager {
  private static final StatisticsTypeFactory tf = StatisticsTypeFactoryImpl.singleton();
  private final List<Statistics> statsList = new CopyOnWriteArrayList<>();
  private final long systemId;
  private final String systemName;
  private final long startTime;
  private final AtomicLong statsListUniqueId = new AtomicLong(1);
  private final boolean statsDisabled =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "statsDisabled");
  private int statsListModCount = 0;

  public StatisticsRegistry(long systemId, String systemName, long startTime) {
    this.systemId = systemId;
    this.systemName = systemName;
    this.startTime = startTime;
  }

  /**
   * Returns the id of this connection to the distributed system. This is actually the port of the
   * distribution manager's distribution channel.
   *
   */
  @Override
  public long getId() {
    return systemId;
  }

  @Override
  public String getName() {
    return systemName;
  }

  public Statistics findStatisticsByUniqueId(final long uniqueId) {
    for (Statistics s : this.statsList) {
      if (uniqueId == s.getUniqueId()) {
        return s;
      }
    }
    return null;
  }

  /**
   * For every registered statistic instance call the specified visitor. This method was added to
   * fix bug 40358
   */
  public void visitStatistics(StatisticsVisitor visitor) {
    for (Statistics s : statsList) {
      visitor.visit(s);
    }
  }

  public int getStatListModCount() {
    return statsListModCount;
  }

  public List<Statistics> getStatsList() {
    return statsList;
  }

  @Override
  public int getStatisticsCount() {
    return statsList.size();
  }

  @Override
  public Statistics findStatistics(long id) {
    for (Statistics s : statsList) {
      if (s.getUniqueId() == id) {
        return s;
      }
    }
    throw new RuntimeException(
        "Could not find statistics instance");
  }

  @Override
  public boolean statisticsExists(long id) {
    for (Statistics s : statsList) {
      if (s.getUniqueId() == id) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Statistics[] getStatistics() {
    return statsList.toArray(new Statistics[0]);
  }

  public Statistics createStatistics(StatisticsType type) {
    return createOsStatistics(type, null, 0, 0);
  }

  public Statistics createStatistics(StatisticsType type, String textId) {
    return createOsStatistics(type, textId, 0, 0);
  }

  public Statistics createStatistics(StatisticsType type, String textId, long numericId) {
    return createOsStatistics(type, textId, numericId, 0);
  }

  public Statistics createOsStatistics(StatisticsType type, String textId, long numericId,
                                         int osStatFlags) {
    if (statsDisabled) {
      return new DummyStatisticsImpl(type, textId, numericId);
    }
    long myUniqueId = statsListUniqueId.getAndIncrement();
    Statistics result =
        new LocalStatisticsImpl(type, textId, numericId, myUniqueId, false, osStatFlags, this);
    synchronized (statsList) {
      statsList.add(result);
      statsListModCount++;
    }
    return result;
  }

  public Statistics[] findStatisticsByType(final StatisticsType type) {
    final ArrayList hits = new ArrayList();
    visitStatistics(new StatisticsVisitor() {
      public void visit(Statistics s) {
        if (type == s.getType()) {
          hits.add(s);
        }
      }
    });
    Statistics[] result = new Statistics[hits.size()];
    return (Statistics[]) hits.toArray(result);
  }

  public Statistics[] findStatisticsByTextId(final String textId) {
    final ArrayList hits = new ArrayList();
    visitStatistics(new StatisticsVisitor() {
      public void visit(Statistics s) {
        if (s.getTextId().equals(textId)) {
          hits.add(s);
        }
      }
    });
    Statistics[] result = new Statistics[hits.size()];
    return (Statistics[]) hits.toArray(result);
  }

  public Statistics[] findStatisticsByNumericId(final long numericId) {
    final ArrayList hits = new ArrayList();
    visitStatistics(new StatisticsVisitor() {
      public void visit(Statistics s) {
        if (numericId == s.getNumericId()) {
          hits.add(s);
        }
      }
    });
    Statistics[] result = new Statistics[hits.size()];
    return (Statistics[]) hits.toArray(result);
  }

  /**
   * for internal use only. Its called by {@link LocalStatisticsImpl#close}.
   */
  public void destroyStatistics(Statistics stats) {
    synchronized (statsList) {
      if (statsList.remove(stats)) {
        statsListModCount++;
      }
    }
  }

  public Statistics createAtomicStatistics(StatisticsType type) {
    return createAtomicStatistics(type, null, 0);
  }

  public Statistics createAtomicStatistics(StatisticsType type, String textId) {
    return createAtomicStatistics(type, textId, 0);
  }

  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId) {
    if (statsDisabled) {
      return new DummyStatisticsImpl(type, textId, numericId);
    }

    long myUniqueId = statsListUniqueId.getAndIncrement();
    Statistics result = StatisticsImpl.createAtomicNoOS(type, textId, numericId, myUniqueId, this);
    synchronized (statsList) {
      statsList.add(result);
      statsListModCount++;
    }
    return result;
  }

  /**
   * Creates or finds a StatisticType for the given shared class.
   */
  public StatisticsType createType(String name, String description, StatisticDescriptor[] stats) {
    return tf.createType(name, description, stats);
  }

  public StatisticsType findType(String name) {
    return tf.findType(name);
  }

  public StatisticsType[] createTypesFromXml(Reader reader) throws IOException {
    return tf.createTypesFromXml(reader);
  }

  public StatisticDescriptor createIntCounter(String name, String description, String units) {
    return tf.createIntCounter(name, description, units);
  }

  public StatisticDescriptor createLongCounter(String name, String description, String units) {
    return tf.createLongCounter(name, description, units);
  }

  public StatisticDescriptor createDoubleCounter(String name, String description, String units) {
    return tf.createDoubleCounter(name, description, units);
  }

  public StatisticDescriptor createIntGauge(String name, String description, String units) {
    return tf.createIntGauge(name, description, units);
  }

  public StatisticDescriptor createLongGauge(String name, String description, String units) {
    return tf.createLongGauge(name, description, units);
  }

  public StatisticDescriptor createDoubleGauge(String name, String description, String units) {
    return tf.createDoubleGauge(name, description, units);
  }

  public StatisticDescriptor createIntCounter(String name, String description, String units,
                                                boolean largerBetter) {
    return tf.createIntCounter(name, description, units, largerBetter);
  }

  public StatisticDescriptor createLongCounter(String name, String description, String units,
                                                 boolean largerBetter) {
    return tf.createLongCounter(name, description, units, largerBetter);
  }

  public StatisticDescriptor createDoubleCounter(String name, String description, String units,
                                                   boolean largerBetter) {
    return tf.createDoubleCounter(name, description, units, largerBetter);
  }

  public StatisticDescriptor createIntGauge(String name, String description, String units,
                                              boolean largerBetter) {
    return tf.createIntGauge(name, description, units, largerBetter);
  }

  public StatisticDescriptor createLongGauge(String name, String description, String units,
                                               boolean largerBetter) {
    return tf.createLongGauge(name, description, units, largerBetter);
  }

  public StatisticDescriptor createDoubleGauge(String name, String description, String units,
                                                 boolean largerBetter) {
    return tf.createDoubleGauge(name, description, units, largerBetter);
  }

  public long getStartTime() {
    return startTime;
  }

  public boolean statsDisabled() {
    return statsDisabled;
  }
}
