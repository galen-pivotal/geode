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

  public boolean isRunning() {
    return true;
  }

  protected abstract void doOneMessage();

  // protected abstract Object poll();
  //
  // protected abstract Object take();

  protected abstract void registerWithSelector2(Selector s) throws IOException;

  public abstract void handleTermination();

  public abstract void registerWithSelector() throws IOException;

  /**
   * Safe to ignore if you don't use run(). Don't worry about why it's a boolean. If you care you
   * can return false if you were already closed.
   */
  public abstract boolean cleanup();

  // todo: setNotProcessingMessage

  @Override
  public void run() {
    if (acceptor.isSelector()) {
      runSingleMessage();
    } else {
      runAsThread();
    }
  }

  private void runSingleMessage() {
    try {
      // this.stats.decThreadQueueSize();
      // if (!isTerminated()) {
      // Message.setTLCommBuffer(this.acceptor.takeCommBuffer());
      if (isRunning()) {
        doOneMessage();
        registerWithSelector(); // finished msg so reregister
      }
    } catch (CancelException ignore) {
      // TODO : do we really need CancelException?
      // ok shutting down
      // ok shutting down
      // } catch (IOException ex) {
      // logger.warn(
      // LocalizedMessage.create(LocalizedStrings.ServerConnection_0__UNEXPECTED_EXCEPTION, ex));
      // setClientDisconnectedException(ex);
      // } finally {
      // this.acceptor.releaseCommBuffer(Message.setTLCommBuffer(null));
      // DistributedSystem.releaseThreadsSockets();
      // unsetOwner();
      // setNotProcessingMessage();
      // unset request specific timeout
      // this.unsetRequestSpecificTimeout(); todo?
      // if (!finishedMsg) {
      // try {
      // handleTermination();
      // } catch (CancelException e) {
      // ignore
      // }
      // }
      // }
    } catch (IOException e) {
      logger.error(e.toString());
    }
  }

  private void runAsThread() {
    try {
      while (isRunning()) {
        try {
          doOneMessage();
        } catch (CancelException ignore) {
        }
        // allow finally block to handle termination
        // } finally {
        // this.unsetRequestSpecificTimeout();
        // Breadcrumbs.clearBreadcrumb();
        // }
        // }
        // } finally {
        // try {
        // this.unsetRequestSpecificTimeout();
        // handleTermination();
        // DistributedSystem.releaseThreadsSockets();
        // } catch (CancelException e) {
        // ignore
        // }
      }
    } finally {
      cleanup();
    }
  }

}
