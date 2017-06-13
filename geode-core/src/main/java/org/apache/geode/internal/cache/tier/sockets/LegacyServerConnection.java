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

package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.internal.AbstractOp;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.InternalClientMembership;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.command.Default;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.Principal;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;

public class LegacyServerConnection extends ServerConnection {
  /**
   * Set to false once handshake has been done
   */
  private boolean waitingForHandshake = true;
  private final Object handShakeMonitor = new Object();
  private Map commands;
  private int failureCount = 0;
  private Random randomConnectionIdGen = null;

  /**
   * Creates a new <code>ServerConnection</code> that processes messages received from an edge
   * client over a given <code>Socket</code>.
   *
   * @param s
   * @param c
   * @param helper
   * @param stats
   * @param hsTimeout
   * @param socketBufferSize
   * @param communicationModeStr
   * @param communicationMode
   * @param acceptor
   */
  public LegacyServerConnection(Socket s, InternalCache c, CachedRegionHelper helper,
      CacheServerStats stats, int hsTimeout, int socketBufferSize, String communicationModeStr,
      byte communicationMode, Acceptor acceptor, SecurityService securityService) {
    super(s, c, helper, stats, hsTimeout, socketBufferSize, communicationModeStr, communicationMode,
        acceptor, securityService);
    this.randomConnectionIdGen = new Random(this.hashCode());
  }

  private boolean processHandShake() {
    boolean result = false;
    boolean clientJoined = false;
    boolean registerClient = false;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      synchronized (getCleanupTable()) {
        Counter numRefs = (Counter) getCleanupTable().get(this.handshake);
        byte epType = (byte) 0;
        int qSize = 0;

        if (this.proxyId.isDurable()) {
          if (isDebugEnabled) {
            logger.debug("looking if the Proxy existed for this durable client or not :{}",
                this.proxyId);
          }
          CacheClientProxy proxy =
              getAcceptor().getCacheClientNotifier().getClientProxy(this.proxyId);
          if (proxy != null && proxy.waitRemoval()) {
            proxy = getAcceptor().getCacheClientNotifier().getClientProxy(this.proxyId);
          }
          if (proxy != null) {
            if (isDebugEnabled) {
              logger.debug("Proxy existed for this durable client :{} and proxy : {}", this.proxyId,
                  proxy);
            }
            if (proxy.isPrimary()) {
              epType = (byte) 2;
              qSize = proxy.getQueueSize();
            } else {
              epType = (byte) 1;
              qSize = proxy.getQueueSize();
            }
          }
          // Bug Fix for 37986
          if (numRefs == null) {
            // Check whether this is a durable client first. A durable client with
            // the same id is not allowed. In this case, reject the client.
            if (proxy != null && !proxy.isPaused()) {
              // The handshake refusal message must be smaller than 127 bytes.
              String handshakeRefusalMessage =
                  LocalizedStrings.ServerConnection_DUPLICATE_DURABLE_CLIENTID_0
                      .toLocalizedString(proxyId.getDurableId());
              logger.warn(LocalizedMessage.create(LocalizedStrings.TWO_ARG_COLON,
                  new Object[] {this.name, handshakeRefusalMessage}));
              refuseHandshake(handshakeRefusalMessage,
                  HandShake.REPLY_EXCEPTION_DUPLICATE_DURABLE_CLIENT);
              return result;
            }
          }
        }
        if (numRefs != null) {
          if (acceptHandShake(epType, qSize)) {
            numRefs.incr();
            this.incedCleanupTableRef = true;
            result = true;
          }
          return result;
        } else {
          if (acceptHandShake(epType, qSize)) {
            clientJoined = true;
            numRefs = new Counter();
            getCleanupTable().put(this.handshake, numRefs);
            numRefs.incr();
            this.incedCleanupTableRef = true;
            this.stats.incCurrentClients();
            result = true;
          }
          return result;
        }
      } // sync
    } // try
    finally {
      if (isTerminated() || result == false) {
        return false;
      }
      synchronized (getCleanupProxyIdTable()) {
        Counter numRefs = (Counter) getCleanupProxyIdTable().get(this.proxyId);
        if (numRefs != null) {
          numRefs.incr();
        } else {
          registerClient = true;
          numRefs = new Counter();
          numRefs.incr();
          getCleanupProxyIdTable().put(this.proxyId, numRefs);
          InternalDistributedMember idm =
              (InternalDistributedMember) this.proxyId.getDistributedMember();
        }
        this.incedCleanupProxyIdTableRef = true;
      }

      if (isDebugEnabled) {
        logger.debug("{}registering client {}", (registerClient ? "" : "not "), proxyId);
      }
      this.crHelper.checkCancelInProgress(null);
      if (clientJoined && isFiringMembershipEvents()) {
        // This is a new client. Notify bridge membership and heartbeat monitor.
        InternalClientMembership.notifyClientJoined(this.proxyId.getDistributedMember());
      }

      ClientHealthMonitor chm = this.acceptor.getClientHealthMonitor();
      synchronized (this.clientHealthMonitorLock) {
        this.clientHealthMonitorRegistered = true;
      }
      if (registerClient) {
        // hitesh: it will add client
        chm.registerClient(this.proxyId);
      }
      // hitesh:it will add client connection in set
      chm.addConnection(this.proxyId, this);
      this.acceptor.getConnectionListener().connectionOpened(registerClient, communicationMode);
      // Hitesh: add user creds in map for single user case.
    } // finally
  }

  private boolean verifyClientConnection() {
    synchronized (this.handShakeMonitor) {
      if (this.handshake == null) {
        // synchronized (getCleanupTable()) {
        boolean readHandShake = ServerHandShakeProcessor.readHandShake(this, getSecurityService());
        if (readHandShake) {
          if (this.handshake.isOK()) {
            try {
              return processHandShake();
            } catch (CancelException e) {
              if (!crHelper.isShutdown()) {
                logger.warn(LocalizedMessage.create(
                    LocalizedStrings.ServerConnection_0_UNEXPECTED_CANCELLATION, getName()), e);
              }
              cleanup();
              return false;
            }
          } else {
            this.crHelper.checkCancelInProgress(null); // bug 37113?
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.ServerConnection_0_RECEIVED_UNKNOWN_HANDSHAKE_REPLY_CODE_1,
                new Object[] {this.name, new Byte(this.handshake.getCode())}));
            refuseHandshake(LocalizedStrings.ServerConnection_RECEIVED_UNKNOWN_HANDSHAKE_REPLY_CODE
                .toLocalizedString(), ServerHandShakeProcessor.REPLY_INVALID);
            return false;
          }
        } else {
          this.stats.incFailedConnectionAttempts();
          cleanup();
          return false;
        }
        // }
      }
    }
    return true;
  }

  protected void doOneMessage() {
    if (this.waitingForHandshake) {
      doHandshake();
      this.waitingForHandshake = false;
    } else {
      this.resetTransientData();
      doNormalMsg();
    }
  }

  private void doNormalMsg() {
    Message msg = null;
    msg = BaseCommand.readRequest(this);
    ThreadState threadState = null;
    try {
      if (msg != null) {
        // Since this thread is not interrupted when the cache server is shutdown, test again after
        // a message has been read. This is a bit of a hack. I think this thread should be
        // interrupted, but currently AcceptorImpl doesn't keep track of the threads that it
        // launches.
        if (!this.processMessages || (crHelper.isShutdown())) {
          if (logger.isDebugEnabled()) {
            logger.debug("{} ignoring message of type {} from client {} due to shutdown.",
                getName(), MessageType.getString(msg.getMessageType()), this.proxyId);
          }
          return;
        }

        if (msg.getMessageType() != MessageType.PING) {
          // check for invalid number of message parts
          if (msg.getNumberOfParts() <= 0) {
            failureCount++;
            if (failureCount > 3) {
              this.processMessages = false;
              return;
            } else {
              return;
            }
          }
        }

        if (logger.isTraceEnabled()) {
          logger.trace("{} received {} with txid {}", getName(),
              MessageType.getString(msg.getMessageType()), msg.getTransactionId());
          if (msg.getTransactionId() < -1) { // TODO: why is this happening?
            msg.setTransactionId(-1);
          }
        }

        if (msg.getMessageType() != MessageType.PING) {
          // we have a real message (non-ping),
          // so let's call receivedPing to let the CHM know client is busy
          acceptor.getClientHealthMonitor().receivedPing(this.proxyId);
        }
        Command command = getCommand(Integer.valueOf(msg.getMessageType()));
        if (command == null) {
          command = Default.getCommand();
        }

        // if a subject exists for this uniqueId, binds the subject to this thread so that we can do
        // authorization later
        if (AcceptorImpl.isIntegratedSecurity() && !isInternalMessage()
            && this.communicationMode != Acceptor.GATEWAY_TO_GATEWAY) {
          long uniqueId = getUniqueId();
          Subject subject = this.clientUserAuths.getSubject(uniqueId);
          if (subject != null) {
            threadState = securityService.bindSubject(subject);
          }
        }

        command.execute(msg, this, this.securityService);
      }
    } finally {
      // Keep track of the fact that a message is no longer being
      // processed.
      setNotProcessingMessage();
      clearRequestMsg();
      if (threadState != null) {
        threadState.clear();
      }
    }
  }

  /**
   * MessageType of the messages (typically internal commands) which do not need to participate in
   * security should be added in the following if block.
   *
   * @return Part
   * @see AbstractOp#processSecureBytes(Connection, Message)
   * @see AbstractOp#needsUserId()
   * @see AbstractOp#sendMessage(Connection)
   */
  @Override
  public Part updateAndGetSecurityPart() {
    // need to take care all message types here
    if (AcceptorImpl.isAuthenticationRequired()
        && this.handshake.getVersion().compareTo(Version.GFE_65) >= 0
        && (this.communicationMode != Acceptor.GATEWAY_TO_GATEWAY)
        && (!this.requestMsg.getAndResetIsMetaRegion()) && (!isInternalMessage())) {
      setSecurityPart();
      return this.securePart;
    } else {
      if (AcceptorImpl.isAuthenticationRequired() && logger.isDebugEnabled()) {
        logger.debug(
            "ServerConnection.updateAndGetSecurityPart() not adding security part for msg type {}",
            MessageType.getString(this.requestMsg.messageType));
      }
    }
    return null;
  }


  private boolean isInternalMessage() {
    return (this.requestMsg.messageType == MessageType.CLIENT_READY
        || this.requestMsg.messageType == MessageType.CLOSE_CONNECTION
        || this.requestMsg.messageType == MessageType.GETCQSTATS_MSG_TYPE
        || this.requestMsg.messageType == MessageType.GET_CLIENT_PARTITION_ATTRIBUTES
        || this.requestMsg.messageType == MessageType.GET_CLIENT_PR_METADATA
        || this.requestMsg.messageType == MessageType.INVALID
        || this.requestMsg.messageType == MessageType.MAKE_PRIMARY
        || this.requestMsg.messageType == MessageType.MONITORCQ_MSG_TYPE
        || this.requestMsg.messageType == MessageType.PERIODIC_ACK
        || this.requestMsg.messageType == MessageType.PING
        || this.requestMsg.messageType == MessageType.REGISTER_DATASERIALIZERS
        || this.requestMsg.messageType == MessageType.REGISTER_INSTANTIATORS
        || this.requestMsg.messageType == MessageType.REQUEST_EVENT_VALUE
        || this.requestMsg.messageType == MessageType.ADD_PDX_TYPE
        || this.requestMsg.messageType == MessageType.GET_PDX_ID_FOR_TYPE
        || this.requestMsg.messageType == MessageType.GET_PDX_TYPE_BY_ID
        || this.requestMsg.messageType == MessageType.SIZE
        || this.requestMsg.messageType == MessageType.TX_FAILOVER
        || this.requestMsg.messageType == MessageType.TX_SYNCHRONIZATION
        || this.requestMsg.messageType == MessageType.GET_FUNCTION_ATTRIBUTES
        || this.requestMsg.messageType == MessageType.ADD_PDX_ENUM
        || this.requestMsg.messageType == MessageType.GET_PDX_ID_FOR_ENUM
        || this.requestMsg.messageType == MessageType.GET_PDX_ENUM_BY_ID
        || this.requestMsg.messageType == MessageType.GET_PDX_TYPES
        || this.requestMsg.messageType == MessageType.GET_PDX_ENUMS
        || this.requestMsg.messageType == MessageType.COMMIT
        || this.requestMsg.messageType == MessageType.ROLLBACK);
  }

  private void setSecurityPart() {
    try {
      this.connectionId = randomConnectionIdGen.nextLong();
      this.securePart = new Part();
      byte[] id = encryptId(this.connectionId, this);
      this.securePart.setPartState(id, false);
    } catch (Exception ex) {
      logger.warn(LocalizedMessage
          .create(LocalizedStrings.ServerConnection_SERVER_FAILED_TO_ENCRYPT_DATA_0, ex));
      throw new GemFireSecurityException("Server failed to encrypt response message.");
    }
  }

  @Override
  protected boolean doAcceptHandShake(byte epType, int qSize) {
    try {
      this.handshake.accept(theSocket.getOutputStream(), theSocket.getInputStream(), epType, qSize,
          this.communicationMode, this.principal);
    } catch (IOException ioe) {
      if (!crHelper.isShutdown() && !isTerminated()) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.ServerConnection_0_HANDSHAKE_ACCEPT_FAILED_ON_SOCKET_1_2,
            new Object[] {this.name, this.theSocket, ioe}));
      }
      cleanup();
      return false;
    }
    return true;
  }

  private void initializeCommands() {
    // The commands are cached here, but are just referencing the ones
    // stored in the CommandInitializer
    this.commands = CommandInitializer.getCommands(this);
  }

  private Command getCommand(Integer messageType) {

    Command cc = (Command) this.commands.get(messageType);
    return cc;
  }

  protected void setProxyId(ClientProxyMembershipID proxyId) {
    this.proxyId = proxyId;
    this.memberIdByteArray = EventID.getMembershipId(proxyId);
    // LogWriterI18n log = InternalDistributedSystem.getLoggerI18n();
    // byte[] oldIdArray = proxyId.getMembershipByteArray();
    // log.warning(LocalizedStrings.DEBUG, "Size comparison for " + proxyId.getDistributedMember()
    // + " old=" + oldIdArray.length + " new=" + memberIdByteArray.length
    // + " diff=" + (oldIdArray.length - memberIdByteArray.length));
    this.name = "Server connection from [" + proxyId + "; port=" + this.theSocket.getPort() + "]";
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


  public byte[] setCredentials(Message msg) throws Exception {

    try {
      // need to get connection id from secure part of message, before that need to insure
      // encryption of id
      // need to check here, whether it matches with serverConnection id or not
      // need to decrpt bytes if its in DH mode
      // need to get properties of credentials(need to remove extra stuff if something is there from
      // client)
      // need to generate unique-id for client
      // need to send back in response with encrption
      if (!AcceptorImpl.isAuthenticationRequired() && msg.isSecureMode()) {
        // TODO (ashetkar)
        /*
         * This means that client and server VMs have different security settings. The server does
         * not have any security settings specified while client has.
         *
         * Here, should we just ignore this and send the dummy security part (connectionId, userId)
         * in the response (in this case, client needs to know that it is not expected to read any
         * security part in any of the server response messages) or just throw an exception
         * indicating bad configuration?
         */
        // This is a CREDENTIALS_NORMAL case.;
        return new byte[0];
      }
      if (!msg.isSecureMode()) {
        throw new AuthenticationFailedException("Authentication failed");
      }

      byte[] secureBytes = msg.getSecureBytes();

      secureBytes = ((HandShake) this.handshake).decryptBytes(secureBytes);

      // need to decrypt it first then get connectionid
      AuthIds aIds = new AuthIds(secureBytes);

      long connId = aIds.getConnectionId();

      if (connId != this.connectionId) {
        throw new AuthenticationFailedException("Authentication failed");
      }


      byte[] credBytes = msg.getPart(0).getSerializedForm();

      credBytes = ((HandShake) this.handshake).decryptBytes(credBytes);

      ByteArrayInputStream bis = new ByteArrayInputStream(credBytes);
      DataInputStream dinp = new DataInputStream(bis);
      Properties credentials = DataSerializer.readProperties(dinp);

      // When here, security is enfored on server, if login returns a subject, then it's the newly
      // integrated security, otherwise, do it the old way.
      long uniqueId;

      DistributedSystem system = this.getDistributedSystem();
      String methodName = system.getProperties().getProperty(SECURITY_CLIENT_AUTHENTICATOR);

      Object principal = HandShake.verifyCredentials(methodName, credentials,
          system.getSecurityProperties(), (InternalLogWriter) system.getLogWriter(),
          (InternalLogWriter) system.getSecurityLogWriter(), this.proxyId.getDistributedMember(),
          this.securityService);
      if (principal instanceof Subject) {
        Subject subject = (Subject) principal;
        uniqueId = this.clientUserAuths.putSubject(subject);
        logger.info(this.clientUserAuths);
      } else {
        // this sets principal in map as well....
        uniqueId = ServerHandShakeProcessor.getUniqueId(this, (Principal) principal);
      }

      // create secure part which will be send in respones
      return encryptId(uniqueId, this);
    } catch (AuthenticationFailedException afe) {
      throw afe;
    } catch (AuthenticationRequiredException are) {
      throw are;
    } catch (Exception e) {
      throw new AuthenticationFailedException("REPLY_REFUSED", e);
    }
  }

  public boolean removeUserAuth(Message msg, boolean keepalive) {
    try {
      byte[] secureBytes = msg.getSecureBytes();

      secureBytes = ((HandShake) this.handshake).decryptBytes(secureBytes);

      // need to decrypt it first then get connectionid
      AuthIds aIds = new AuthIds(secureBytes);

      long connId = aIds.getConnectionId();

      if (connId != this.connectionId) {
        throw new AuthenticationFailedException("Authentication failed");
      }

      try {
        // first try integrated security
        boolean removed = this.clientUserAuths.removeSubject(aIds.getUniqueId());

        // if not successfull, try the old way
        if (!removed)
          removed = this.clientUserAuths.removeUserId(aIds.getUniqueId(), keepalive);
        return removed;

      } catch (NullPointerException npe) {
        // Bug #52023.
        logger.debug("Exception {}", npe);
        return false;
      }
    } catch (Exception ex) {
      throw new AuthenticationFailedException("Authentication failed", ex);
    }
  }
}
