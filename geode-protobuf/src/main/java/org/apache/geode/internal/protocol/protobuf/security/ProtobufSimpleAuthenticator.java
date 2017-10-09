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

import org.apache.geode.internal.protocol.protobuf.AuthenticationAPI;
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
  @Override
  public Subject authenticate(InputStream inputStream, OutputStream outputStream,
      SecurityService securityService) throws IOException, AuthenticationFailedException {
    AuthenticationAPI.SimpleAuthenticationRequest authenticationRequest =
        AuthenticationAPI.SimpleAuthenticationRequest.parseDelimitedFrom(inputStream);
    if (authenticationRequest == null) {
      throw new EOFException();
    }

    Properties properties = new Properties();
    properties.putAll(authenticationRequest.getCredentialsMap());

    // throws AuthenticationFailedException on failure.
    Subject subject = securityService.login(properties);

    AuthenticationAPI.SimpleAuthenticationResponse.newBuilder().setAuthenticated(true)
        .build().writeDelimitedTo(outputStream);

    return subject;
  }
}
