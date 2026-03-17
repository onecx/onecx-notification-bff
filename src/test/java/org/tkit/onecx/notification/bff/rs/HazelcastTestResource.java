package org.tkit.onecx.notification.bff.rs;

import java.util.Map;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Starts a local, standalone Hazelcast instance before the Quarkus application
 * boots in tests. This satisfies the {@code Hazelcast.getAllHazelcastInstances()}
 * call in {@link org.tkit.onecx.notification.bff.rs.service.NotificationClusterService#onStart},
 * which would otherwise find an empty set and throw a {@link java.util.NoSuchElementException}.
 *
 * Multicast and TCP discovery are disabled so the instance stays completely
 * local and does not attempt to form a real cluster during tests.
 */
public class HazelcastTestResource implements QuarkusTestResourceLifecycleManager {

    private HazelcastInstance instance;

    @Override
    public Map<String, String> start() {
        Config config = new Config();
        config.setClusterName("onecx-notification-cluster-test");
        // Disable all network discovery — single local node only
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        // Suppress noisy Hazelcast startup logs in test output
        config.setProperty("hazelcast.logging.type", "slf4j");

        instance = Hazelcast.newHazelcastInstance(config);
        return Map.of();
    }

    @Override
    public void stop() {
        if (instance != null) {
            instance.shutdown();
        }
    }
}
