package org.apache.geode;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;

public class EchoNettyChannelHandler extends ChannelInboundHandlerAdapter {
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    System.out.println(
        "Thread.currentThread().getName() = " + Thread
            .currentThread().getName() + "+ Thread.currentThread().getID() = " + Thread.currentThread().getId());


    ctx.fireChannelRead(msg);
  }

//  @Override
//  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//    ctx.flush();
//  }
}
