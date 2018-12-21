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
 *
 */
package org.apache.geode.internal.statistics.micrometer;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import org.apache.geode.internal.statistics.MeterServer;

/**
 * A meter server that, when sampled, delivers measurements in Prometheus format.
 */
public class PrometheusMeterServer implements MeterServer {
  private final PrometheusMeterRegistry registry;
  private HttpsServer httpsServer;

  public PrometheusMeterServer(PrometheusMeterRegistry registry) {
    this.registry = registry;
  }

  @Override
  public void start() {
    final int port = 8000;
    System.out.println("Starting PrometheusMeterServer on port " + port);

    try {
      // setup the socket address
      InetSocketAddress address = new InetSocketAddress(port);

      // initialise the HTTPS server
      httpsServer = HttpsServer.create(address, 0);
      SSLContext sslContext = SSLContext.getInstance("TLS");

      // initialise the keystore
      char[] password = "password".toCharArray();
      KeyStore ks = KeyStore.getInstance("JKS");
      FileInputStream fis = new FileInputStream("/Users/mhanson/testkey.jks");
      ks.load(fis, password);

      // setup the key manager factory
      KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
      kmf.init(ks, password);

      // setup the trust manager factory
      TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
      tmf.init(ks);

      // setup the HTTPS context and parameters
      sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
      httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
        public void configure(HttpsParameters params) {
          try {
            // initialise the SSL context
            SSLContext c = getSSLContext();
            SSLEngine engine = c.createSSLEngine();
            params.setNeedClientAuth(false);
            params.setCipherSuites(engine.getEnabledCipherSuites());
            params.setProtocols(engine.getEnabledProtocols());

            // Set the SSL parameters
            SSLParameters sslParameters = c.getSupportedSSLParameters();
            params.setSSLParameters(sslParameters);

          } catch (Exception ex) {
            System.out.println("Failed to create HTTPS port");
          }
        }
      });

      httpsServer.createContext("/", t -> {

        String response = registry.scrape();
        t.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        t.sendResponseHeaders(200, response.getBytes().length);
        OutputStream os = t.getResponseBody();
        os.write(response.getBytes());
        System.out.println("Got a callback");
        os.close();
      });
      httpsServer.setExecutor(null); // creates a default executor
      httpsServer.start();
      System.out.println("httpsServer started on port " + port);

    } catch (Exception exception) {
      System.out.println("Failed to create HTTPS server on port " + port + " of localhost");
    }

  }

  @Override
  public void stop() {
    System.out.println("Stopping PrometheusMeterServer");

    if (httpsServer != null) {
      httpsServer.stop(0);
      httpsServer = null;
      System.out.println("Stopped PrometheusMeterServer");
    }
  }
}
