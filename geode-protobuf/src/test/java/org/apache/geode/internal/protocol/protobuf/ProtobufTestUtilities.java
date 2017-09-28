package org.apache.geode.internal.protocol.protobuf;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.protobuf.GeneratedMessageV3;

public class ProtobufTestUtilities {
  public static ByteArrayInputStream messageToByteArrayInputStream(GeneratedMessageV3 message)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    message.writeDelimitedTo(byteArrayOutputStream);
    return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
  }


  public static ClientProtocol.Request createProtobufRequestWithGetRegionNamesRequest(
      RegionAPI.GetRegionNamesRequest getRegionNamesRequest) {
    return ClientProtocol.Request.newBuilder().setGetRegionNamesRequest(getRegionNamesRequest)
        .build();
  }

  public static void verifyHandshake(InputStream inputStream, OutputStream outputStream,
      HandshakeAPI.AuthenticationMode authenticationMode) throws IOException {
    buildHandshakeRequest(authenticationMode).writeDelimitedTo(outputStream);

    HandshakeAPI.HandshakeResponse handshakeResponse =
        HandshakeAPI.HandshakeResponse.parseDelimitedFrom(inputStream);

    assertTrue(handshakeResponse.getOk());
    assertFalse(handshakeResponse.hasError());
  }

  public static HandshakeAPI.HandshakeRequest buildHandshakeRequest(
      HandshakeAPI.AuthenticationMode authenticationMode) {
    return HandshakeAPI.HandshakeRequest.newBuilder()
        .setVersion(HandshakeAPI.Semver.newBuilder().setMajor(1).setMinor(0))
        .setAuthenticationMode(authenticationMode).build();
  }
}
