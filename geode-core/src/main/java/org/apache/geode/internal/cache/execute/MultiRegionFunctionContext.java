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
package org.apache.geode.internal.cache.execute;

import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;

/**
 * Context available when called using {@link InternalFunctionService#onRegions(Set)}
 *
 *
 * @since GemFire 6.5
 *
 */
public interface MultiRegionFunctionContext extends FunctionContext {

  Set<Region> getRegions();

  /**
   * Returns a boolean to identify whether this is a re-execute. Returns true if it is a re-execute
   * else returns false
   *
   * @return a boolean (true) to identify whether it is a re-execute (else false)
   *
   * @since GemFire 6.5
   * @see Function#isHA()
   */
  @Override
  boolean isPossibleDuplicate();

}
