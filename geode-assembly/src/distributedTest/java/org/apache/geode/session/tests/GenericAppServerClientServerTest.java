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
package org.apache.geode.session.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpSession;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.modules.session.functions.GetSessionCount;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

/**
 * Extends the {@link CargoTestBase} class to support client server tests of generic app servers
 *
 * Currently being used to test Jetty 9 containers in client server mode.
 */
public abstract class GenericAppServerClientServerTest extends CargoTestBase {
  private static class ModifyingPrintStream extends PrintStream {
    private String identifier;

    public ModifyingPrintStream(PrintStream stream, String identifier) {
      super(stream);
      this.identifier = identifier;
    }

    private void writeLineStart() {
      super.print(identifier);
    }

    @Override
    public void write(int b) {
      synchronized (this) {
        super.write(b);
      }
    }

    @Override
    public void write(byte[] b) throws IOException {
      synchronized (this) {
        writeLineStart();
        super.write(b);
      }
    }

    @Override
    public void write(byte[] buf, int off, int len) {
      synchronized (this) {
        writeLineStart();
        super.write(buf, off, len);
      }
    }

    @Override
    public void print(boolean b) {
      synchronized (this) {
        writeLineStart();
        super.print(b);
      }
    }

    @Override
    public void print(char c) {
      synchronized (this) {
        writeLineStart();
        super.print(c);
      }
    }

    @Override
    public void print(int i) {
      synchronized (this) {
        writeLineStart();
        super.print(i);
      }
    }

    @Override
    public void print(long l) {
      synchronized (this) {
        writeLineStart();
        super.print(l);
      }
    }

    @Override
    public void print(float f) {
      synchronized (this) {
        writeLineStart();
        super.print(f);
      }
    }

    @Override
    public void print(double d) {
      synchronized (this) {
        writeLineStart();
        super.print(d);
      }
    }

    @Override
    public void print(char[] s) {
      synchronized (this) {
        writeLineStart();
        super.print(s);
      }
    }

    @Override
    public void print(String s) {
      synchronized (this) {
        writeLineStart();
        super.print(s);
      }
    }

    @Override
    public void print(Object obj) {
      synchronized (this) {
        writeLineStart();
        super.print(obj);
      }
    }

    @Override
    public void println() {
      synchronized (this) {
        writeLineStart();
        super.println();
      }
    }

    @Override
    public void println(boolean x) {
      synchronized (this) {
        writeLineStart();
        super.println(x);
      }
    }

    @Override
    public void println(char x) {
      synchronized (this) {
        writeLineStart();
        super.println(x);
      }
    }

    @Override
    public void println(int x) {
      synchronized (this) {
        writeLineStart();
        super.println(x);
      }
    }

    @Override
    public void println(long x) {
      synchronized (this) {
        writeLineStart();
        super.println(x);
      }
    }

    @Override
    public void println(float x) {
      synchronized (this) {
        writeLineStart();
        super.println(x);
      }
    }

    @Override
    public void println(double x) {
      synchronized (this) {
        writeLineStart();
        super.println(x);
      }
    }

    @Override
    public void println(char[] x) {
      synchronized (this) {
        writeLineStart();
        super.println(x);
      }
    }

    @Override
    public void println(String x) {
      synchronized (this) {
        writeLineStart();
        super.println(x);
      }
    }

    @Override
    public void println(Object x) {
      synchronized (this) {
        writeLineStart();
        super.println(x);
      }
    }

    @Override
    public PrintStream printf(String format, Object... args) {
      synchronized (this) {
        writeLineStart();
        return super.printf(format, args);
      }
    }

    @Override
    public PrintStream printf(Locale l, String format, Object... args) {
      synchronized (this) {
        writeLineStart();
        return super.printf(l, format, args);
      }
    }

    @Override
    public PrintStream format(String format, Object... args) {
      synchronized (this) {
        writeLineStart();
        return super.format(format, args);
      }
    }

    @Override
    public PrintStream format(Locale l, String format, Object... args) {
      synchronized (this) {
        writeLineStart();
        return super.format(l, format, args);
      }
    }

    @Override
    public PrintStream append(CharSequence csq) {
      synchronized (this) {
        writeLineStart();
        return super.append(csq);
      }
    }

    @Override
    public PrintStream append(CharSequence csq, int start, int end) {
      synchronized (this) {
        writeLineStart();
        return super.append(csq, start, end);
      }
    }

    @Override
    public PrintStream append(char c) {
      synchronized (this) {
        writeLineStart();
        return super.append(c);
      }
    }
  }

  private static PrintStream originalOut;

  @BeforeClass
  public static void monkeyPatchStdout() {
    originalOut = System.out;
    System.setOut(new ModifyingPrintStream(originalOut,
        "[TAG " + ManagementFactory.getRuntimeMXBean().getName() + " ]"));
  }

  @AfterClass
  public static void restoreStdout() {
    if (originalOut != null) {
      System.setOut(originalOut);
      originalOut = null;
    }
  }



  protected VM serverVM;

  /**
   * Starts the server for the client containers to connect to while testing.
   */
  @Before
  public void startServers() throws InterruptedException {
    // Setup host
    Host host = Host.getHost(0);
    serverVM = host.getVM(0);
    serverVM.invoke(() -> {
      Cache cache = getCache();
      // Add cache server
      CacheServer server = cache.addCacheServer();
      server.setPort(0);
      // Start the server in this VM
      server.start();
    });
  }

  /**
   * Test that we don't leave native sessions in the container, wasting memory
   */
  @Test
  public void shouldNotLeaveNativeSessionInContainer()
      throws IOException, URISyntaxException, InterruptedException {
    manager.startAllInactiveContainers();

    String key = "value_testSessionExpiration";
    String value = "Foo";

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals(value, resp.getResponse());
    }

    for (int i = 0; i < manager.numContainers(); i++) {
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.executionFunction(GetSessionCount.class);
      assertEquals("Should have 0 native sessions", "0", resp.getResponse());
    }
  }

  @Override
  protected void verifySessionIsRemoved(String key) throws IOException, URISyntaxException {
    serverVM.invoke(() -> {
      Cache cache = getCache();
      Region region = cache.getRegion("gemfire_modules_sessions");
      Awaitility.await().atMost(1, TimeUnit.MINUTES)
          .untilAsserted(() -> assertEquals(0, region.size()));
    });
    super.verifySessionIsRemoved(key);
  }

  @Override
  protected void verifyMaxInactiveInterval(int expected) throws IOException, URISyntaxException {
    super.verifyMaxInactiveInterval(expected);
    serverVM.invoke(() -> {
      Cache cache = getCache();
      Region<Object, HttpSession> region =
          cache.<Object, HttpSession>getRegion("gemfire_modules_sessions");
      region.values().forEach(session -> assertEquals(expected, session.getMaxInactiveInterval()));
    });
  }
}
