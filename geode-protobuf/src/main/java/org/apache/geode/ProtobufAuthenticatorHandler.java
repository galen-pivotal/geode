package org.apache.geode;

import java.io.ByteArrayOutputStream;
import java.util.Properties;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.protocol.protobuf.AuthenticationAPI;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.protocol.protobuf.ProtobufSimpleAuthenticator;
import org.apache.geode.protocol.protobuf.ProtobufSimpleAuthorizer;
import org.apache.geode.redis.internal.ByteToCommandDecoder;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.SecurityManager;

public class ProtobufAuthenticatorHandler extends ByteToCommandDecoder {
  private final AttributeKey<Object>
      securityPrincipalKey =
      AttributeKey.newInstance("security-principal");
  private final SecurityManager securityManager;

  public ProtobufAuthenticatorHandler(SecurityManager securityManager) {
    this.securityManager = securityManager;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    Channel channel = ctx.channel();
    if (channel.hasAttr(securityPrincipalKey)) {
      ctx.fireChannelRead(msg);
      return;
    }

    ByteBuf buf = (ByteBuf) msg;

    AuthenticationAPI.SimpleAuthenticationRequest
        authenticationRequest =
        AuthenticationAPI.SimpleAuthenticationRequest.parseFrom(buf.array());

    Properties properties = new Properties();
    properties.setProperty(ResourceConstants.USER_NAME, authenticationRequest.getUsername());
    properties.setProperty(ResourceConstants.PASSWORD, authenticationRequest.getPassword());

//    authorizer = null; // authenticating a new user clears current authorizer
    try {
      Object principal = securityManager.authenticate(properties);
//      if (principal != null) {
//        authorizer = new ProtobufSimpleAuthorizer(principal, securityManager);
//      }

      Attribute<Object> attr = channel.attr(securityPrincipalKey);
      attr.set(principal);

      ctx.writeAndFlush(
          AuthenticationAPI.SimpleAuthenticationResponse.newBuilder().setAuthenticated(true));

      ReferenceCountUtil.release(buf);
    } catch (AuthenticationFailedException e) {
//      authorizer = null;
      ctx.close();
    }
    return;

//
//    securityManager

  }
//    ClientProtocol.Message message = (ClientProtocol.Message) msg;
}
