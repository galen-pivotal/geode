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

package org.apache.geode.internal.net.runnable;

import org.apache.geode.CancelException;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.channels.Selector;


/**
 * Hopefully this utility class will be something that future network protocol implementations can
 * inherit from.
 */
public abstract class AcceptorConnection implements Runnable {
  protected final AcceptorImpl acceptor;
  protected final Logger logger;

  protected AcceptorConnection(AcceptorImpl acceptor, Logger logger) {
    this.acceptor = acceptor;
    this.logger = logger;
  }


  protected abstract void doOneMessage();

  // protected abstract Object poll();
  //
  // protected abstract Object take();

  protected abstract void registerWithSelector2(Selector s) throws IOException;

  public abstract void handleTermination();

  public abstract void registerWithSelector() throws IOException;

  public abstract void emergencyClose();

  /**
   * Safe to ignore if you don't use run(). Don't worry about why it's a boolean. If you care you
   * can return false if you were already closed.
   */
  public abstract boolean cleanup();

  public abstract ClientProxyMembershipID getProxyID();

  public abstract String getSocketHost();

  public abstract int getSocketPort();

  public abstract int getClientReadTimeout();

  // todo: setNotProcessingMessage



}
