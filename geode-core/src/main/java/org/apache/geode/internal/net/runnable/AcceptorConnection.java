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

//  protected abstract Object poll();
//
//  protected abstract Object take();

  protected abstract void registerWithSelector2(Selector s) throws IOException;

  public abstract void handleTermination();

  protected abstract void registerWithSelector() throws IOException;

  // safe to ignore if you don't use run().
  protected abstract void releaseResources();

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
//        this.stats.decThreadQueueSize();
//        if (!isTerminated()) {
//          Message.setTLCommBuffer(this.acceptor.takeCommBuffer());
      if (isRunning()) {
        doOneMessage();
        registerWithSelector(); // finished msg so reregister
      }
    } catch (CancelException ignore) {
      // TODO : do we really need CancelException?
      // ok shutting down
      // ok shutting down
//      } catch (IOException ex) {
//        logger.warn(
//          LocalizedMessage.create(LocalizedStrings.ServerConnection_0__UNEXPECTED_EXCEPTION, ex));
//        setClientDisconnectedException(ex);
//      } finally {
//        this.acceptor.releaseCommBuffer(Message.setTLCommBuffer(null));
      // DistributedSystem.releaseThreadsSockets();
//        unsetOwner();
//        setNotProcessingMessage();
      // unset request specific timeout
//        this.unsetRequestSpecificTimeout(); todo?
//        if (!finishedMsg) {
//          try {
//            handleTermination();
//          } catch (CancelException e) {
//             ignore
//          }
//        }
//      }
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
//        } finally {
//          this.unsetRequestSpecificTimeout();
//          Breadcrumbs.clearBreadcrumb();
//        }
//        }
//    } finally {
//      try {
//        this.unsetRequestSpecificTimeout();
//        handleTermination();
//        DistributedSystem.releaseThreadsSockets();
//      } catch (CancelException e) {
        // ignore
//      }
      }
    } finally {
      releaseResources();
    }
  }

}
