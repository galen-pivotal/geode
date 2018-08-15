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
package org.apache.geode.distributed;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class ServerCyclingSplitBrainTest {
  private int[] ports;
  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  private static final int SHUTDOWNS_PER_VM = 10;
  private List<MemberVM> locators;

  @Before
  public void setUp() {
    startLocators(4);
  }

  @Test
  public void serversKeepLocksDuringRecovery() throws Exception {
    // TODO: kill DUnit locator
    final List<Future<?>> locatorFutures = new ArrayList<>();
    assertClusterConnected(locators);

    for (int i = 0; i < locators.size(); i++) {
      final int myPort = ports[i];
      final MemberVM thisLocator = locators.get(i);
      final int []ports = this.ports;
      locatorFutures.add(thisLocator.invokeAsync(() -> restartRepeatedly(myPort, ports)));
    }

    Awaitility.await().atMost(1, TimeUnit.MINUTES)
        .until(() -> locatorFutures.stream().allMatch(Future::isDone));

    for (Future future : locatorFutures) {
      future.get(); // throws if failure.
    }

    assertClusterConnected(locators);
    System.out.println("finished");
  }

  private void assertClusterConnected(List<MemberVM> locators) {
    final List<Integer> membersInEachVM = locators.parallelStream()
        .map((vm) -> vm.invoke(ServerCyclingSplitBrainTest::numberOfOtherMembers))
        .collect(Collectors.toList());

    assertArrayEquals("members per VM" + Arrays.toString(membersInEachVM.toArray()),
        new Integer[]{3, 3, 3, 3}, membersInEachVM.toArray());
  }

  private void startLocators(int n) {
    ports = AvailablePortHelper.getRandomAvailableTCPPorts(n);
    locators = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final int myPort = ports[i];
      final int[] ports = this.ports;
      locators.add(clusterStartupRule.startLocatorVM(i,
          getLocatorStarterRuleSerializableFunction1(myPort, ports)));
    }

    assertClusterConnected(locators);
  }

  private static ClusterStartupRule.SerializableFunction1<LocatorStarterRule> getLocatorStarterRuleSerializableFunction1(
      int myPort, int[] ports) {
    return locatorStarterRule -> locatorStarterRule.withPort(myPort)
        .withConnectionToLocator(ports);
  }


  private static void restartRepeatedly(int port, int[] locatorPorts) {
    final LocatorStarterRule memberStarter = (LocatorStarterRule) ClusterStartupRule.memberStarter;
    for (int shutdowns = 0; shutdowns < SHUTDOWNS_PER_VM; ) {
      System.out.println("shutdowns = " + shutdowns);
      final Locator locator = Locator.getLocator();
      final InternalDistributedSystem distributedSystem =
          (InternalDistributedSystem) locator.getDistributedSystem();
      final DistributionManager distributionManager = distributedSystem.getDistributionManager();

      System.out.println("all other members size = " + distributedSystem.getAllOtherMembers().size());
      if (distributedSystem.getAllOtherMembers().size() > 0 &&
        distributionManager.getElderId().equals(distributionManager.getId())) {
          shutdowns++;
          System.out.println("I'm elder");
          distributedSystem.disconnect();

          memberStarter
              .withPort(port)
              .withConnectionToLocator(locatorPorts)
              .startLocator();
          final InternalDistributedSystem newDistributedSystem = (InternalDistributedSystem) Locator.getLocator().getDistributedSystem();
          assertTrue(newDistributedSystem.isConnected());

      } else {
        // Assert that we haven't hit split brain.
        assertTrue(distributedSystem.isConnected());
      }
    }
  }


  private static int numberOfOtherMembers() {
    final DistributedSystem distributedSystem = Locator.getLocator().getDistributedSystem();
    Awaitility.await().atMost(10, SECONDS).until(distributedSystem::isConnected);
    return distributedSystem.getAllOtherMembers().size();
  }
}
