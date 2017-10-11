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
package org.apache.geode.internal.protocol.protobuf.security;

import static org.apache.geode.internal.protocol.protobuf.ProtocolErrorCode.AUTHENTICATION_FAILED;

import org.apache.geode.internal.protocol.protobuf.AuthenticationAPI;

import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.AuthenticationFailedException;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.shiro.subject.Subject;

public class ProtobufSimpleAuthenticator implements Authenticator {

  public static final String SHOULD_HAVE_AUTHED =
      "Got non-auth request while expecting authentication request";

  @Override
  public Subject authenticate(InputStream inputStream, OutputStream outputStream,
      SecurityService securityService) throws IOException, AuthenticationFailedException {
    ClientProtocol.Message message = ClientProtocol.Message.parseDelimitedFrom(inputStream);

    if (message.getRequest().getRequestAPICase()
        .getNumber() != ClientProtocol.Request.SIMPLEAUTHENTICATIONREQUEST_FIELD_NUMBER) {
      failAuth(outputStream);
    }

    AuthenticationAPI.SimpleAuthenticationRequest authenticationRequest =
        message.getRequest().getSimpleAuthenticationRequest();
    if (authenticationRequest == null) {
      failAuth(outputStream);
    }

    Properties properties = new Properties();
    properties.putAll(authenticationRequest.getCredentialsMap());

    // throws AuthenticationFailedException on failure.
    Subject authToken = securityService.login(properties);

    AuthenticationAPI.SimpleAuthenticationResponse.newBuilder().setAuthenticated(true).build()
        .writeDelimitedTo(outputStream);

    return authToken;
  }

  private void failAuth(OutputStream outputStream) throws IOException {
    BasicTypes.Error.newBuilder().setErrorCode(AUTHENTICATION_FAILED.codeValue)
        .setMessage(SHOULD_HAVE_AUTHED).build().writeDelimitedTo(outputStream);

    throw new IOException(SHOULD_HAVE_AUTHED);
  }
}
