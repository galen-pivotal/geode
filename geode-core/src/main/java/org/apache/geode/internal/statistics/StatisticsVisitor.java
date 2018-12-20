package org.apache.geode.internal.statistics;

import org.apache.geode.Statistics;

/**
 * Used to "visit" each instance of Statistics registered with
 *
 * @see org.apache.geode.internal.statistics.StatisticsRegistry#visitStatistics
 */
public interface StatisticsVisitor {

  void visit(Statistics stat);
}
