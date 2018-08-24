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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ConcurrencyRule;

public class GrantorFailoverDUnitTest {
  private final List<MemberVM> locators = new ArrayList<>();

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public ConcurrencyRule concurrencyRule = new ConcurrencyRule();
  public static final String SERVICE_NAME = "serviceName";

  @Before
  public void before() {
    locators.add(clusterStartupRule.startLocatorVM(0));
    locators.add(clusterStartupRule.startLocatorVM(1, locators.get(0).getPort()));
    locators.add(clusterStartupRule.startLocatorVM(2, locators.get(0).getPort()));
  }

  @After
  public void cleanup() {
    // for (MemberVM locator: locators) {
    // locator.invoke(() -> DistributedLockService.destroy(SERVICE_NAME));
    // }
  }

  @Test
  public void cannotUnlockALockLockedByAnotherVm() throws Exception {
    final String lock0 = "lock 0";
    final String lock1 = "lock 1";
    for (MemberVM locator : locators) {
      concurrencyRule.add(() -> {
        locator.invoke((SerializableRunnableIF) () -> DistributedLockService.create(SERVICE_NAME,
            ClusterStartupRule.getCache().getDistributedSystem()));
        return null;
      });
    }
    concurrencyRule.executeInParallel();

    concurrencyRule.add(() -> locators.get(0).invoke(() -> DistributedLockService.getServiceNamed(
        SERVICE_NAME).lock(
            lock0, 20, -1)))
        .expectValue(Boolean.TRUE);
    concurrencyRule.add(() -> locators.get(1).invoke(() -> DistributedLockService.getServiceNamed(
        SERVICE_NAME).lock(
            lock1, 20, -1)))
        .expectValue(Boolean.TRUE);
    concurrencyRule.executeInParallel();

    concurrencyRule.add(() -> locators.get(1).invoke(() -> DistributedLockService.getServiceNamed(
        SERVICE_NAME).lock(
            lock0, 20, -1)))
        .expectValue(Boolean.FALSE);
    concurrencyRule.add(() -> locators.get(0).invoke(() -> DistributedLockService.getServiceNamed(
        SERVICE_NAME).lock(
            lock1, 20, -1)))
        .expectValue(Boolean.FALSE);
    concurrencyRule.executeInParallel();

    final AsyncInvocation unlock1 = locators.get(0)
        .invokeAsync(() -> DistributedLockService.getServiceNamed(SERVICE_NAME).unlock(lock1));
    final AsyncInvocation unlock0 = locators.get(1)
        .invokeAsync(() -> DistributedLockService.getServiceNamed(SERVICE_NAME).unlock(lock0));

    assertThatThrownBy(unlock0::get).hasCauseInstanceOf(LockNotHeldException.class);
    assertThatThrownBy(unlock1::get).hasCauseInstanceOf(LockNotHeldException.class);
  }

  @Test
  public void lockRecoveryAfterGrantorDies() throws Exception {
    final String lock1 = "lock 1";
    final String lock2 = "lock 2";

    locators.get(0).invoke(GrantorFailoverDUnitTest::assertIsElderAndGetId);

    for (MemberVM locator : locators) {
      locator.invoke((SerializableRunnableIF) () -> DistributedLockService.create(SERVICE_NAME,
          ClusterStartupRule.getCache().getDistributedSystem()));
    }

    // Grantor but not the elder
    final MemberVM grantorVM = locators.get(1);
    final MemberVM survivor1 = locators.get(0);
    final MemberVM survivor2 = locators.get(2);
    grantorVM.invoke(() -> DistributedLockService.becomeLockGrantor(SERVICE_NAME));

    concurrencyRule.add(() -> survivor1
        .invoke(() -> DistributedLockService.getServiceNamed(SERVICE_NAME).lock(lock1, 20_000, -1)))
        .expectValue(Boolean.TRUE);
    concurrencyRule.add(() -> survivor2
        .invoke(() -> DistributedLockService.getServiceNamed(SERVICE_NAME).lock(lock2, 20_000, -1)))
        .expectValue(Boolean.TRUE);
    concurrencyRule.executeInParallel();

    clusterStartupRule.crashVM(1);

    locators.remove(grantorVM);

    // can't get the locks again
    concurrencyRule
        .add(() -> survivor2
            .invoke(() -> DistributedLockService.getServiceNamed(SERVICE_NAME).lock(lock1, 2, -1)))
        .expectValue(Boolean.FALSE);
    concurrencyRule
        .add(() -> survivor1
            .invoke(() -> DistributedLockService.getServiceNamed(SERVICE_NAME).lock(lock2, 2, -1)))
        .expectValue(Boolean.FALSE);
    concurrencyRule.executeInParallel();

    final AsyncInvocation lock1FailsReleaseOnOtherVM =
        survivor2
            .invokeAsync(() -> DistributedLockService.getServiceNamed(SERVICE_NAME).unlock(lock1));
    final AsyncInvocation lock2FailsReleaseOnOtherVM =
        survivor1
            .invokeAsync(() -> DistributedLockService.getServiceNamed(SERVICE_NAME).unlock(lock2));


    assertThatThrownBy(lock1FailsReleaseOnOtherVM::get)
        .hasRootCauseInstanceOf(LockNotHeldException.class);
    assertThatThrownBy(lock2FailsReleaseOnOtherVM::get)
        .hasRootCauseInstanceOf(LockNotHeldException.class);

    final AsyncInvocation lock1SuccessfulRelease =
        survivor1
            .invokeAsync(() -> DistributedLockService.getServiceNamed(SERVICE_NAME).unlock(lock1));

    final AsyncInvocation lock2SuccessfulRelease =
        survivor2
            .invokeAsync(() -> DistributedLockService.getServiceNamed(SERVICE_NAME).unlock(lock1));

    lock1SuccessfulRelease.get();
    lock2SuccessfulRelease.get();
  }

  private static InternalDistributedMember assertIsElderAndGetId() {
    DistributionManager distributionManager =
        ClusterStartupRule.getCache().getInternalDistributedSystem().getDistributionManager();
    assertThat(distributionManager.isElder()).isTrue();
    return distributionManager.getElderId();
  }
}
