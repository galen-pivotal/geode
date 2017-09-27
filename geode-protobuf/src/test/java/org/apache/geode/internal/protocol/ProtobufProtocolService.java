package org.apache.geode.internal.protocol;

import java.util.Map;

import org.apache.geode.internal.cache.tier.sockets.ClientProtocolHandshaker;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolMessageHandler;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolService;
import org.apache.geode.internal.protocol.protobuf.Handshaker;
import org.apache.geode.internal.protocol.protobuf.ProtobufStreamProcessor;
import org.apache.geode.security.server.Authenticator;

public class ProtobufProtocolService implements ClientProtocolService {
  @Override
  public ClientProtocolHandshaker getHandshaker(Map<String, Authenticator> availableAuthenticators) {
    return new Handshaker(availableAuthenticators);
  }

  @Override
  public ClientProtocolMessageHandler getMessageHandler() {
    return new ClientProtocolMessageHandler() {
    };
  }
}
