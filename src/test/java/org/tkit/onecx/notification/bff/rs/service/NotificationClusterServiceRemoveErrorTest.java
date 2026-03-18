package org.tkit.onecx.notification.bff.rs.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.tkit.onecx.notification.bff.rs.AbstractTest;
import org.tkit.onecx.notification.bff.rs.ws.NotificationSockJSBridge;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;

/**
 * Uses {@link RemoveErrorProfile} to activate {@link FailingRemoveClusterService} as a CDI
 * alternative, making every {@code removeFromMap()} call return a failing Uni.
 *
 * Scenario:
 * 1. A client connects via WebSocket and sends REGISTER — hasActiveReceiver() becomes true.
 * 2. A notification is stored via storeNotification(), which publishes to the Hazelcast topic.
 * 3. The topic listener fires on this pod, sees hasActiveReceiver=true, calls removeFromMap().
 * 4. Notification is still delivered to the WebSocket (EventBus publish happens before removeFromMap).
 */
@QuarkusTest
@TestProfile(NotificationClusterServiceRemoveErrorTest.RemoveErrorProfile.class)
@SuppressWarnings("deprecation")
class NotificationClusterServiceRemoveErrorTest extends AbstractTest {

    public static class RemoveErrorProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(FailingRemoveClusterService.class);
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of();
        }
    }

    @Inject
    NotificationClusterService service;

    @Inject
    Vertx vertx;

    @TestHTTPResource("/")
    URL baseUrl;

    /**
     * Notification is still delivered to the WebSocket — the EventBus publish happens before
     * removeFromMap() is called, so the failure does not suppress delivery.
     */
    @Test
    void topicListener_removeFromMapFails_errorHandlerCalled_notificationStillDelivered() throws Exception {
        String receiverId = "remove-error-receiver";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        List<String> received = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(new WebSocketConnectOptions()
                .setHost(baseUrl.getHost())
                .setPort(baseUrl.getPort())
                .setURI("/eventbus/websocket"))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        WebSocket ws = ar.result();
                        wsHolder.add(ws);
                        ws.handler(buf -> {
                            String text = buf.toString();
                            if (!text.equals("h") && !text.equals("o")) {
                                received.add(text);
                            }
                        });
                        openLatch.countDown();
                        ws.writeTextMessage(new JsonObject()
                                .put("type", "register")
                                .put("address", address)
                                .encode());
                    }
                });

        assertThat(openLatch.await(5, TimeUnit.SECONDS)).isTrue();

        // Wait until the bridge marks the receiver as active
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue());

        // Store a notification — topic listener fires, sees hasActiveReceiver=true,
        NotificationDTO dto = new NotificationDTO();
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(false);
        service.storeNotification(dto).await().indefinitely();

        // Notification must still arrive — EventBus publish happens before removeFromMap()
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isNotEmpty());
        assertThat(received.get(0)).contains(receiverId);

        // Connection must remain open — error handler does not close it
        assertThat(wsHolder.get(0).isClosed()).isFalse();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }
}