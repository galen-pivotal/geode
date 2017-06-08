package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.tier.Acceptor;

/**
 * Created by gosullivan on 6/7/17.
 */
public class LegacyMessageHandler implements MessageHandler {
  /**
   * Set to false once handshake has been done
   */
  private boolean doHandshake = true;

  private void doOneMessage() {
    if (this.doHandshake) {
      doHandshake();
      this.doHandshake = false;
    } else {
      this.resetTransientData();
      doNormalMsg();
    }
  }

  private void doHandshake() {
    // hitesh:to create new connection handshake
    if (verifyClientConnection()) {
      // Initialize the commands after the handshake so that the version
      // can be used.
      initializeCommands();
      // its initialized in verifyClientConnection call
      if (getCommunicationMode() != Acceptor.GATEWAY_TO_GATEWAY)
        initializeClientUserAuths();
    }
    if (TEST_VERSION_AFTER_HANDSHAKE_FLAG) {
      Assert.assertTrue((this.handshake.getVersion().ordinal() == testVersionAfterHandshake),
        "Found different version after handshake");
      TEST_VERSION_AFTER_HANDSHAKE_FLAG = false;
    }
  }

  void resetTransientData() {
    this.potentialModification = false;
    this.requiresResponse = false;
    this.responded = false;
    this.requiresChunkedResponse = false;
    this.modKey = null;
    this.modRegion = null;

    queryResponseMsg.setNumberOfParts(2);
    chunkedResponseMsg.setNumberOfParts(1);
    executeFunctionResponseMsg.setNumberOfParts(1);
    registerInterestResponseMsg.setNumberOfParts(1);
    keySetResponseMsg.setNumberOfParts(1);
  }
}
