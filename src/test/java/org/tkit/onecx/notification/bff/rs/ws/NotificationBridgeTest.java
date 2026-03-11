package org.tkit.onecx.notification.bff.rs.ws;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.tkit.onecx.notification.bff.rs.AbstractTest;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.IssuerDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.SeverityDTO;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.keycloak.client.KeycloakTestClient;
import io.restassured.http.ContentType;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;


//@QuarkusTest
class NotificationBridgeTest extends AbstractTest {

//    @TestHTTPResource("/")
//    URI baseUri;
//
//    @Inject
//    Vertx vertx;
//
//    private final KeycloakTestClient keycloakClient = new KeycloakTestClient();
//
//    @Test
//    void bridge_delivers_notification_to_registered_client() throws Exception {
//        String receiverId = "bridge-test-user-" + System.nanoTime();
//        String ebAddress = "notifications.new." + receiverId;
//
//        CountDownLatch openLatch = new CountDownLatch(1);
//        CountDownLatch messageLatch = new CountDownLatch(1);
//        AtomicReference<String> receivedBody = new AtomicReference<>();
//
//        // Vert.x 5: WebSocket connections use a dedicated WebSocketClient
//        WebSocketClient wsClient = vertx.createWebSocketClient();
//
//        WebSocketConnectOptions wsOptions = new WebSocketConnectOptions()
//                .setHost(baseUri.getHost())
//                .setPort(baseUri.getPort())
//                .setURI("/eventbus/websocket");
//
//        wsClient.connect(wsOptions).onComplete(result -> {
//            if (result.failed()) {
//                openLatch.countDown(); // unblock so the test can fail fast
//                return;
//            }
//            var ws = result.result();
//
//            ws.textMessageHandler(text -> {
//                if ("o".equals(text)) {
//                    // SockJS open frame — register our address
//                    openLatch.countDown();
//                    JsonObject registerFrame = new JsonObject()
//                            .put("type", "register")
//                            .put("address", ebAddress);
//                    ws.writeTextMessage("[\"" + registerFrame.encode().replace("\"", "\\\"") + "\"]");
//                } else if (text.startsWith("a[")) {
//                    // SockJS array frame — EventBus message delivered
//                    receivedBody.set(text);
//                    messageLatch.countDown();
//                }
//            });
//        });
//
//        // Wait for SockJS open + register
//        assertThat(openLatch.await(10, TimeUnit.SECONDS))
//                .as("SockJS socket did not open in time").isTrue();
//
//        // Dispatch via REST — triggers EventBus publish in NotificationClusterService
//        NotificationDTO notification = new NotificationDTO()
//                .applicationId("bridge-test-app")
//                .senderId("sender-bridge")
//                .receiverId(receiverId)
//                .persist(false)
//                .severity(SeverityDTO.CRITICAL)
//                .issuer(IssuerDTO.SYSTEM);
//
//        given()
//                .auth().oauth2(keycloakClient.getAccessToken(ADMIN))
//                .contentType(ContentType.JSON)
//                .body(notification)
//                .when()
//                .post("/notifications/dispatch")
//                .then()
//                .statusCode(200);
//
//        // Assert the bridge pushed the EventBus message to the client within 10 s
//        assertThat(messageLatch.await(10, TimeUnit.SECONDS))
//                .as("SockJS bridge did not deliver the notification in time").isTrue();
//        assertThat(receivedBody.get())
//                .as("Received frame should contain the receiverId")
//                .contains(receiverId);
//
//        wsClient.close();
//    }
}
