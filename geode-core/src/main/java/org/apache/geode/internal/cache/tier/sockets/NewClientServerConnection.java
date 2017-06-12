package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.ServiceLoader;

public class NewClientServerConnection extends ServerConnection {
  // The new protocol lives in a separate module and gets loaded when this class is instantiated.
  private final ClientProtocolMessageHandler newClientProtocol;


  /**
   * Creates a new <code>NewClientServerConnection</code> that processes messages received from an
   * edge client over a given <code>Socket</code>.
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
  public NewClientServerConnection(Socket s, Cache c, CachedRegionHelper helper,
      CacheServerStats stats, int hsTimeout, int socketBufferSize, String communicationModeStr,
      byte communicationMode, Acceptor acceptor, ClientProtocolMessageHandler newClientProtocol) {
    super(s, c, helper, stats, hsTimeout, socketBufferSize, communicationModeStr, communicationMode,
        acceptor);
    assert (communicationMode == AcceptorImpl.CLIENT_TO_SERVER_NEW_PROTOCOL);
    this.newClientProtocol = newClientProtocol;
  }

  @Override
  protected void doOneMessage() {
    try {
      Socket socket = this.getSocket();
      InputStream inputStream = socket.getInputStream();
      OutputStream outputStream = socket.getOutputStream();
      // TODO serialization types?
      newClientProtocol.receiveMessage(inputStream, outputStream, this.getCache());
    } catch (IOException e) {
      // TODO?
    }
    return;
  }

  @Override
  protected boolean doAcceptHandShake(byte epType, int qSize) {
    // no handshake for new client protocol.
    return true;
  }

  @Override
  public boolean isClientServerConnection() {
    return true;
  }

  protected boolean createClientHandshake() {
    InetSocketAddress remoteAddress = (InetSocketAddress) getSocket().getRemoteSocketAddress();
    DistributedMember member =
        new InternalDistributedMember(remoteAddress.getAddress(), remoteAddress.getPort());
    this.proxyId = new ClientProxyMembershipID(member);
    this.handshake = new HandShake(this.proxyId, this.getDistributedSystem(), Version.CURRENT);
    return true;
  }


}
