package org.tkit.onecx.notification.bff.rs.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tkit.onecx.notification.bff.rs.AbstractTest;
import org.tkit.onecx.notification.bff.rs.ws.FailingReadObjectMapperProducer;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(NotificationClusterServiceDeserializationErrorTest.DeserializationErrorProfile.class)
class NotificationClusterServiceDeserializationErrorTest extends AbstractTest {

    public static class DeserializationErrorProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(FailingReadObjectMapperProducer.class);
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of();
        }
    }

    @Inject
    NotificationClusterService clusterService;

    @BeforeEach
    void drainAll() {
        clusterService.consumeByReceiverId("deserialize-error-receiver").await().indefinitely();
    }

    @Test
    void topicListener_deserializationFails_catchBlockHit_notificationStillInMap() {
        NotificationDTO dto = new NotificationDTO();
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId("deserialize-error-receiver");
        dto.setPersist(false);

        // storeNotification writes to IMap then publishes to topic
        clusterService.storeNotification(dto).await().indefinitely();

        // The notification is still in the IMap — the map write happens before the topic
        // publish, so it is unaffected by the deserialization failure in the listener
        List<NotificationDTO> stored = clusterService
                .consumeByReceiverId("deserialize-error-receiver")
                .await().indefinitely();
        assertThat(stored).hasSize(1);
        assertThat(stored.get(0).getReceiverId()).isEqualTo("deserialize-error-receiver");
    }
}
