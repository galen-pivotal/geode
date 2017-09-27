package org.apache.geode.internal.protocol.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.geode.internal.cache.tier.sockets.ClientProtocolHandshaker;
import org.apache.geode.security.server.Authenticator;

public class Handshaker implements ClientProtocolHandshaker {
  private static final int MAJOR_VERSION = 1;
  private static final int MINOR_VERSION = 0;
  private final Map<String, Authenticator> authenticators;
  private boolean shaken = false;

  public Handshaker(Map<String, Authenticator> availableAuthenticators) {
    this.authenticators = availableAuthenticators;
  }

  @Override
  public Authenticator handshake(InputStream inputStream, OutputStream outputStream)
      throws IOException {
    HandshakeAPI.HandshakeRequest handshakeRequest =
        HandshakeAPI.HandshakeRequest.parseDelimitedFrom(inputStream);

    HandshakeAPI.Semver version = handshakeRequest.getVersion();
    if (version.getMajor() != MAJOR_VERSION || version.getMinor() > MINOR_VERSION) {
      writeFailureTo(outputStream, 42, "Version mismatch");
      throw new IllegalStateException("Major version does not match");
    }

    Authenticator authenticator =
        selectAuthenticator(authenticators, handshakeRequest.getAuthenticationMode());

    if (authenticator == null) {
      writeFailureTo(outputStream, 43, "Invalid authentication mode");
      throw new IllegalStateException("Major version does not match");
    }

    HandshakeAPI.HandshakeResponse.newBuilder().setOk(true).build().writeDelimitedTo(outputStream);
    return authenticator;
  }

  private Authenticator selectAuthenticator(Map<String, Authenticator> authenticators,
      HandshakeAPI.AuthenticationMode authenticationMode) {
    switch (authenticationMode) {
      case SIMPLE:
        return authenticators.get("SIMPLE");
      case NONE:
        return authenticators.get("NOOP");
      case UNRECOGNIZED:
        // fallthrough!
      default:
        return null;
    }
  }

  private void writeFailureTo(OutputStream outputStream, int errorCode, String errorMessage)
      throws IOException {
    HandshakeAPI.HandshakeResponse.newBuilder().setOk(false)
        .setError(BasicTypes.Error.newBuilder().setErrorCode(errorCode).setMessage(errorMessage))
        .build().writeDelimitedTo(outputStream);
  }

  @Override
  public boolean shaken() {
    return shaken;
  }
}
