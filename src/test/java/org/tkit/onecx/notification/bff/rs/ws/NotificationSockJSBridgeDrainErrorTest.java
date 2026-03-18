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

import org.junit.jupiter.api.Test;
import org.tkit.onecx.notification.bff.rs.AbstractTest;
import org.tkit.onecx.notification.bff.rs.service.NotificationClusterService;

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
 * Uses {@link DrainErrorProfile} to activate {@link FailingClusterService} as a CDI
 * alternative, making every {@code consumeByReceiverId()} call return a failing Uni.
 */
@QuarkusTest
@TestProfile(NotificationSockJSBridgeDrainErrorTest.DrainErrorProfile.class)
@SuppressWarnings("deprecation")
class NotificationSockJSBridgeDrainErrorTest extends AbstractTest {

    public static class DrainErrorProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(FailingClusterService.class);
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of();
        }
    }

    @Inject
    Vertx vertx;

    @TestHTTPResource("/")
    URL baseUrl;

    /**
     *
     * consumeByReceiverId() returns a failing Uni (via FailingClusterService),
     * so the error handler is invoked and LOG.errorf is called.
     * The SockJS connection must remain open — bridgeEvent.complete(true) is
     * always called regardless of what happens in the subscribe callbacks.
     */
    @Test
    void sockjs_register_drainFails_errorHandlerCalled_connectionRemainsOpen() throws Exception {
        String receiverId = "ws-drain-error-receiver";
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

        // REGISTER must still complete — hasActiveReceiver is set before the failing drain
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue());

        // No notification pushed — drain failed before any publish
        Thread.sleep(500);
        assertThat(received).isEmpty();

        // Connection must still be open — error handler does not close it
        assertThat(wsHolder.get(0).isClosed()).isFalse();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }
}
