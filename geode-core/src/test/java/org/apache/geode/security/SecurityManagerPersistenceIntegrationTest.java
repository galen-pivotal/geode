package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;

/**
 * This test verifies that when a SecurityManager is set, it does not persist between Cache
 * creations.
 *
 * We had seen this issue, and it broke integration tests.
 */
public class SecurityManagerPersistenceIntegrationTest {
  private static final Properties defaultProperties;
  static {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");

    defaultProperties = properties;
  }

  @Test
  public void doesNotPersistWhenSetViaCacheFactoryJavaApi() {
    CacheFactory firstCacheFactory = new CacheFactory(new Properties(defaultProperties));
    SecurityManager securityManager = new SimpleSecurityManager();
    firstCacheFactory.setSecurityManager(securityManager);

    InternalCache firstCache = (InternalCache) firstCacheFactory.create();
      firstCache.close();

    CacheFactory cacheFactory = new CacheFactory(new Properties(defaultProperties));
    try (InternalCache cache = (InternalCache) cacheFactory.create()) {
      assertCacheHasNoSecurity(cache);
    }
  }

  @Test
  public void doesNotPersistWhenSetWithProperty() {
    Properties properties = new Properties(defaultProperties);
    properties.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());

    CacheFactory firstCacheFactory = new CacheFactory(properties);

    InternalCache firstCache = (InternalCache) firstCacheFactory.create();
    firstCache.close();

    // Make sure we're using a fresh CacheFactory, so the test is valid.
    CacheFactory cacheFactory = new CacheFactory(new Properties(defaultProperties));
    try (InternalCache cache = (InternalCache) cacheFactory.create()) {
      assertCacheHasNoSecurity(cache);
    }
  }

  private void assertCacheHasNoSecurity(InternalCache cache) {
    SecurityService securityService = cache.getSecurityService();

    assertThat(securityService.isIntegratedSecurity()).isFalse();
    assertThat(securityService.isClientSecurityRequired()).isFalse();
    assertThat(securityService.isPeerSecurityRequired()).isFalse();

    // We expect null if it's not set, but there could be a default security manager if
    // implementation details change.
    if (securityService.getSecurityManager() != null) {
      assertThat(securityService.getSecurityManager())
          .describedAs("Security manager is not the same as the previously existing Cache")
          .isNotInstanceOf(SpySecurityManager.class);
    }
  }
}
