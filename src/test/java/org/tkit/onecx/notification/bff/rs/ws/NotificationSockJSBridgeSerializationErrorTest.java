package org.tkit.onecx.notification.bff.rs.ws;

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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tkit.onecx.notification.bff.rs.AbstractTest;
import org.tkit.onecx.notification.bff.rs.service.NotificationClusterService;

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

@QuarkusTest
@TestProfile(NotificationSockJSBridgeSerializationErrorTest.FailingMapperProfile.class)
@SuppressWarnings("deprecation")
class NotificationSockJSBridgeSerializationErrorTest extends AbstractTest {

    /**
     * Activates the {@link FailingObjectMapperProducer} CDI alternative so that
     * every call to {@code objectMapper.writeValueAsString()} throws during this test class.
     */
    public static class FailingMapperProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(FailingObjectMapperProducer.class);
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of();
        }
    }

    @Inject
    NotificationClusterService clusterService;

    @Inject
    Vertx vertx;

    @TestHTTPResource("/")
    URL baseUrl;

    @BeforeEach
    void drainAll() {
        clusterService.consumeByReceiverId("ws-serialize-error-receiver").await().indefinitely();
    }

    @Test
    void sockjs_register_serializationError_catchBlockHit_connectionRemainsOpen() throws Exception {
        String receiverId = "ws-serialize-error-receiver";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        // Store a notification so the drain loop is entered and writeValueAsString is called
        NotificationDTO dto = new NotificationDTO();
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(false);
        clusterService.storeNotification(dto).await().indefinitely();

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

        // Receiver must be tracked as active — REGISTER completed before serialization failed
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue());

        // Wait a moment — no notification should arrive because serialization threw
        Thread.sleep(500);
        assertThat(received).isEmpty();

        // Connection must still be open — the catch block does not close it
        assertThat(wsHolder.get(0).isClosed()).isFalse();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }
}
