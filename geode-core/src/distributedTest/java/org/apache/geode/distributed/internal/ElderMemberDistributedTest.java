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
package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class ElderMemberDistributedTest {
  public List<MemberVM> locators = new ArrayList<>();

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Before
  public void before() {
    locators.add(clusterStartupRule.startLocatorVM(0));
    locators.add(clusterStartupRule.startLocatorVM(1, locators.get(0).getPort()));
    locators.add(clusterStartupRule.startLocatorVM(2, locators.get(0).getPort()));
  }

  @Test
  public void oldestMemberIsElder() {
    final InternalDistributedMember elderId = locators.get(0).invoke(
        ElderMemberDistributedTest::assertIsElderAndGetId);

    locators.get(1).invoke(() -> elderConsistencyCheck(elderId));

    locators.get(2).invoke(() -> elderConsistencyCheck(elderId));

    clusterStartupRule.crashVM(0);

    final InternalDistributedMember newElderId = locators.get(1).invoke(
        ElderMemberDistributedTest::assertIsElderAndGetId);
    locators.get(2).invoke(() -> elderConsistencyCheck(newElderId));
  }

  private static InternalDistributedMember assertIsElderAndGetId() {
    DistributionManager distributionManager =
        ClusterStartupRule.getCache().getInternalDistributedSystem().getDistributionManager();
    assertThat(distributionManager.isElder()).isTrue();
    return distributionManager.getElderId();
  }

  private static void elderConsistencyCheck(InternalDistributedMember elderId) {
    DistributionManager distributionManager =
        ClusterStartupRule.getCache().getInternalDistributedSystem().getDistributionManager();
    assertThat(distributionManager.isElder()).isFalse();
    assertThat(distributionManager.getElderId()).isEqualTo(elderId);
  }
}
