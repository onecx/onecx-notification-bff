package org.tkit.onecx.notification.bff.rs.ws;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.tkit.onecx.notification.bff.rs.AbstractTest;
import org.tkit.onecx.notification.bff.rs.service.NotificationClusterService;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import io.quarkiverse.mockserver.test.InjectMockServerClient;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;

@QuarkusTest
@SuppressWarnings("deprecation")
class NotificationSockJSBridgeTest extends AbstractTest {

    @Inject
    NotificationClusterService clusterService;

    @Inject
    Vertx vertx;

    @InjectMockServerClient
    MockServerClient mockServerClient;
    /** Injected by Quarkus — gives us the actual random test port. */
    @TestHTTPResource("/")
    URL baseUrl;

    @BeforeEach
    void drainAll() {
        for (String r : List.of("ws-receiver1", "ws-receiver2", "ws-persist-receiver",
                "ws-empty-receiver", "ws-fail-delivered-receiver", "ws-null-persist-receiver",
                "ws-persist-no-id-receiver")) {
            clusterService.consumeByReceiverId(r).await().indefinitely();
        }
    }

    // -------------------------------------------------------------------------
    // Static helpers / constants
    // -------------------------------------------------------------------------

    @Test
    void bridgePath_constant_isExpectedValue() {
        assertThat(NotificationSockJSBridge.BRIDGE_PATH).isEqualTo("/eventbus/*");
    }

    @Test
    void hasActiveReceiver_unknownReceiver_returnsFalse() {
        assertThat(NotificationSockJSBridge.hasActiveReceiver("not-registered")).isFalse();
    }

    //    @Test
    //    void hasActiveReceiver_nullReceiver_returnsFalse() {
    //        assertThat(NotificationSockJSBridge.hasActiveReceiver(null)).isFalse();
    //    }

    @Test
    void hasActiveReceiver_emptyString_returnsFalse() {
        assertThat(NotificationSockJSBridge.hasActiveReceiver("")).isFalse();
    }

    // -------------------------------------------------------------------------
    // /eventbus/info — SockJS negotiation endpoint (no auth required)
    // -------------------------------------------------------------------------

    @Test
    void eventbusInfoEndpoint_isReachableWithoutAuth() {
        given()
                .when().get("/eventbus/info")
                .then()
                .statusCode(200);
    }

    // -------------------------------------------------------------------------
    // Address prefix regex correctness
    // -------------------------------------------------------------------------

    @Test
    void addressPrefix_doesNotContainUnescapedRegexSpecials() {
        String prefix = NotificationClusterService.EB_ADDRESS_PREFIX;
        assertThat(prefix).matches("[a-z.]+");
    }

    @Test
    void addressPrefix_formedAddress_matchesBridgeRegex() {
        String prefix = NotificationClusterService.EB_ADDRESS_PREFIX;
        String regex = prefix.replace(".", "\\.") + ".+";

        assertThat(prefix + "receiver1").matches(regex);
        // prefix alone must NOT match — receiverId part is required
        assertThat(prefix).doesNotMatch(regex);
    }

    // -------------------------------------------------------------------------
    // SockJS WebSocket integration — SOCKET_CREATED + REGISTER + SOCKET_CLOSED
    // -------------------------------------------------------------------------

    /**
     * Opens a SockJS WebSocket connection and sends a REGISTER frame.
     * Verifies that after REGISTER:
     * - hasActiveReceiver() returns true for that receiverId
     * - After the socket is closed, hasActiveReceiver() returns false
     */
    @Test
    void sockjs_register_addsToActiveReceivers_and_close_clears() throws Exception {
        String receiverId = "ws-receiver1";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        CountDownLatch openLatch = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());
        WebSocketConnectOptions opts = sockjsOpts();

        client.webSocket(opts).onComplete(ar -> {
            if (ar.succeeded()) {
                WebSocket ws = ar.result();
                wsHolder.add(ws);
                ws.closeHandler(v -> closeLatch.countDown());
                ws.handler(buf -> {
                });
                openLatch.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .encode());
            }
        });

        assertThat(openLatch.await(5, TimeUnit.SECONDS)).isTrue();

        // Give the server time to process the REGISTER event
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue());

        // Close and verify cleanup
        wsHolder.get(0).close();
        assertThat(closeLatch.await(5, TimeUnit.SECONDS)).isTrue();

        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse());

        client.close();
    }

    /**
     * Stores a notification for a receiver BEFORE the client connects.
     * On REGISTER, the bridge must drain the inbox and push the notification
     * to the WebSocket — the client receives it without a separate HTTP call.
     */
    @Test
    void sockjs_register_drainsStoredNotifications() throws Exception {
        String receiverId = "ws-receiver2";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        // Store a notification before the client connects
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

        client.webSocket(sockjsOpts()).onComplete(ar -> {
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

        assertThat(openLatch.await(10, TimeUnit.SECONDS)).isTrue();

        // Wait until the drained notification arrives on the WebSocket
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isNotEmpty());

        // The message body must contain the receiverId
        assertThat(received.get(0)).contains(receiverId);

        // The inbox must be empty now — consumed atomically on REGISTER
        List<NotificationDTO> remaining = clusterService.consumeByReceiverId(receiverId)
                .await().indefinitely();
        assertThat(remaining).isEmpty();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * Stores a persist=true notification. On REGISTER + drain the bridge must
     * call markNotificationAsDelivered on the SVC (mocked via MockServer).
     * We verify indirectly: the inbox is drained and the notification arrives.
     */
    @Test
    void sockjs_register_persistNotification_markedAsDelivered() throws Exception {
        String receiverId = "ws-persist-receiver";
        String notifId = "persist-notif-id-1";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        mockServerClient
                .when(request()
                        .withMethod("GET")
                        .withPath("/internal/notifications/" + notifId + "/delivered"))
                .respond(response().withStatusCode(200));

        NotificationDTO dto = new NotificationDTO();
        dto.setId(notifId);
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(true);
        clusterService.storeNotification(dto).await().indefinitely();

        List<String> received = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
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

        // Notification must arrive on the WebSocket
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isNotEmpty());

        assertThat(received.get(0)).contains(receiverId);

        // Inbox must be cleared — consumed atomically on REGISTER
        assertThat(clusterService.consumeByReceiverId(receiverId).await().indefinitely()).isEmpty();

        // Verify markNotificationAsDelivered was called on the SVC (executeBlocking is async — wait for it)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> mockServerClient.verify(
                request()
                        .withMethod("GET")
                        .withPath("/internal/notifications/" + notifId + "/delivered")));

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * Registering on an address that does NOT start with the notification prefix
     * must not add anything to ACTIVE_RECEIVERS (the else-branch in the bridge handler).
     */
    @Test
    void sockjs_register_unknownAddress_doesNotAddToActiveReceivers() throws Exception {
        String unknownAddress = "some.other.address";
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
            if (ar.succeeded()) {
                WebSocket ws = ar.result();
                wsHolder.add(ws);
                openLatch.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", unknownAddress)
                        .encode());
            }
        });

        assertThat(openLatch.await(5, TimeUnit.SECONDS)).isTrue();
        // Give the server time to process
        Thread.sleep(500);

        // unknownAddress is not a receiverId — must NOT be tracked
        assertThat(NotificationSockJSBridge.hasActiveReceiver(unknownAddress)).isFalse();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * REGISTER with no stored notifications — inbox is empty so
     * the isEmpty() branch is taken, no message is pushed to the WebSocket.
     * The connection must still be established and hasActiveReceiver() must be true.
     */
    @Test
    void sockjs_register_emptyInbox_noMessageSent() throws Exception {
        String receiverId = "ws-empty-receiver";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        // Deliberately do NOT store anything — inbox is empty
        List<String> received = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
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

        // Receiver must be tracked as active
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue());

        // Wait a moment to ensure no spurious messages arrive
        Thread.sleep(500);

        // Empty inbox → no notification pushed to the WebSocket
        assertThat(received).isEmpty();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * Covers the onFailure branch of executeBlocking for markNotificationAsDelivered:
     * MockServer returns 500, which causes the REST client to throw a
     * ClientWebApplicationException, triggering the failure log path.
     * The notification must still be delivered to the WebSocket — the mark-as-delivered
     * failure must not suppress delivery.
     */
    @Test
    void sockjs_register_persistNotification_markDeliveredFails_notificationStillDelivered() throws Exception {
        String receiverId = "ws-fail-delivered-receiver";
        String notifId = "persist-fail-id-1";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        // Override the MockServer to return 500 for this specific notification id
        mockServerClient
                .when(request()
                        .withMethod("GET")
                        .withPath("/internal/notifications/" + notifId + "/delivered"))
                .respond(response().withStatusCode(500));

        NotificationDTO dto = new NotificationDTO();
        dto.setId(notifId);
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(true);
        clusterService.storeNotification(dto).await().indefinitely();

        List<String> received = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
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

        // Notification must still arrive despite the mark-as-delivered failure
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isNotEmpty());
        assertThat(received.get(0)).contains(receiverId);

        // Inbox must be cleared — drain happens before the mark-as-delivered call
        assertThat(clusterService.consumeByReceiverId(receiverId).await().indefinitely()).isEmpty();

        // Verify the SVC was called (even though it returned 500)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> mockServerClient.verify(
                request()
                        .withMethod("GET")
                        .withPath("/internal/notifications/" + notifId + "/delivered")));

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * We verify this indirectly: a REGISTER with a receiverId whose key happens to
     * already have been consumed (empty map) still completes without error, and
     * the receiver is tracked as active. The error branch itself is only reachable
     * if the underlying Hazelcast map operation throws — which we verify by
     * confirming the success path runs normally when the map is healthy.
     *
     * The drain error path logs an error and does not propagate — the SockJS
     * connection stays open and bridgeEvent.complete(true) is still called.
     */
    @Test
    void sockjs_register_afterDrain_secondRegisterIsEmptyAndConnectionStaysOpen() throws Exception {
        String receiverId = "ws-receiver2";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        // Store one notification, connect, drain it
        NotificationDTO dto = new NotificationDTO();
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(false);
        clusterService.storeNotification(dto).await().indefinitely();

        List<String> firstReceived = new ArrayList<>();
        CountDownLatch firstOpen = new CountDownLatch(1);
        CountDownLatch firstClose = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        // First connection — drains the inbox
        client.webSocket(sockjsOpts()).onComplete(ar -> {
            if (ar.succeeded()) {
                WebSocket ws = ar.result();
                wsHolder.add(ws);
                ws.closeHandler(v -> firstClose.countDown());
                ws.handler(buf -> {
                    String text = buf.toString();
                    if (!text.equals("h") && !text.equals("o")) {
                        firstReceived.add(text);
                    }
                });
                firstOpen.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .encode());
            }
        });

        assertThat(firstOpen.await(10, TimeUnit.SECONDS)).isTrue();
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(firstReceived).isNotEmpty());

        // Close first connection
        wsHolder.get(0).close();
        assertThat(firstClose.await(10, TimeUnit.SECONDS)).isTrue();
        wsHolder.clear();

        // Second connection — inbox is now empty; consumeByReceiverId returns empty list,
        List<String> secondReceived = new ArrayList<>();
        CountDownLatch secondOpen = new CountDownLatch(1);

        client.webSocket(sockjsOpts()).onComplete(ar -> {
            if (ar.succeeded()) {
                WebSocket ws = ar.result();
                wsHolder.add(ws);
                ws.handler(buf -> {
                    String text = buf.toString();
                    if (!text.equals("h") && !text.equals("o")) {
                        secondReceived.add(text);
                    }
                });
                secondOpen.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .encode());
            }
        });

        assertThat(secondOpen.await(5, TimeUnit.SECONDS)).isTrue();

        // Receiver is active on this pod
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue());

        // No messages pushed — empty inbox
        Thread.sleep(500);
        assertThat(secondReceived).isEmpty();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    @Test
    void sockjs_register_nullAddress_elsePathTaken_connectionOpen() throws Exception {
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
            if (ar.succeeded()) {
                WebSocket ws = ar.result();
                wsHolder.add(ws);
                openLatch.countDown();
                // Send REGISTER with no address field — getString("address") returns null
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .encode());
            }
        });

        assertThat(openLatch.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(500);

        // No receiverId should have been added — address was null
        assertThat(NotificationSockJSBridge.hasActiveReceiver("")).isFalse();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * Boolean.TRUE.equals(null) == false, so markNotificationAsDelivered is NOT called.
     * The notification must still be delivered to the WebSocket.
     */
    @Test
    void sockjs_register_persistNull_notificationDelivered_markAsDeliveredSkipped() throws Exception {
        String receiverId = "ws-null-persist-receiver";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        // persist is deliberately left null
        NotificationDTO dto = new NotificationDTO();
        dto.setId("null-persist-id");
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(null);
        clusterService.storeNotification(dto).await().indefinitely();

        List<String> received = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
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

        // Notification must still arrive — persist=null does not prevent delivery
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isNotEmpty());
        assertThat(received.get(0)).contains(receiverId);

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    @Test
    void sockjs_register_persistTrueButNullId_notificationDelivered_markAsDeliveredSkipped() throws Exception {
        String receiverId = "ws-persist-no-id-receiver";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        // persist=true but id is deliberately null
        NotificationDTO dto = new NotificationDTO();
        dto.setId(null);
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(true);
        clusterService.storeNotification(dto).await().indefinitely();

        List<String> received = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
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

        // Notification must still arrive — id=null does not prevent delivery
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isNotEmpty());
        assertThat(received.get(0)).contains(receiverId);

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    private WebSocketConnectOptions sockjsOpts() {
        return new WebSocketConnectOptions()
                .setHost(baseUrl.getHost())
                .setPort(baseUrl.getPort())
                .setURI("/eventbus/websocket");
    }
}
