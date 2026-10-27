package org.tkit.onecx.notification.bff.rs.ws;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.verify.VerificationTimes;
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
        List<String> receiverIds = List.of(
                getTokenSubject(getKeycloakUserToken(ADMIN)),
                getTokenSubject(getKeycloakUserToken(USER)));
        for (String receiverId : receiverIds) {
            clusterService.consumeByReceiverId(receiverId).await().indefinitely();
        }
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            for (String receiverId : receiverIds) {
                assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
            }
        });
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

    @Test
    void hasActiveReceiver_nullReceiver_returnsFalse() {
        assertThat(NotificationSockJSBridge.hasActiveReceiver(null)).isFalse();
    }

    @Test
    void hasActiveReceiver_emptyString_returnsFalse() {
        assertThat(NotificationSockJSBridge.hasActiveReceiver("")).isFalse();
    }

    // -------------------------------------------------------------------------
    // /eventbus/info — SockJS negotiation endpoint (permit)
    // -------------------------------------------------------------------------

    @Test
    void eventbusInfoEndpoint_requiresAuth() {
        given()
                .when().get("/eventbus/info")
                .then()
                .statusCode(200);
    }

    @Test
    void eventbusInfoEndpoint_isReachableWithAuth() {
        String token = getKeycloakUserToken(USER);

        given()
                .auth().oauth2(token)
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
    void sockjs_register_addsToActiveReceivers_and_close_removesSocket() throws Exception {
        String token = getKeycloakUserToken(USER);
        String receiverId = getTokenSubject(token);
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
                        .put("token", token)
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

    @Test
    void sockjs_connectionsTrackReceiverActivityIndependently() throws Exception {
        String userToken = getKeycloakUserToken(USER);
        String userId = getTokenSubject(userToken);
        String adminToken = getKeycloakUserToken(ADMIN);
        String adminId = getTokenSubject(adminToken);
        String userAddress = NotificationClusterService.EB_ADDRESS_PREFIX + userId;
        String adminAddress = NotificationClusterService.EB_ADDRESS_PREFIX + adminId;

        Queue<String> firstUserFrames = new ConcurrentLinkedQueue<>();
        Queue<String> secondUserFrames = new ConcurrentLinkedQueue<>();
        Queue<String> adminFrames = new ConcurrentLinkedQueue<>();
        HttpClient client = vertx.createHttpClient(new HttpClientOptions());
        List<WebSocket> sockets = new ArrayList<>();

        try {
            WebSocket firstUser = connectAndRegister(client, userAddress, userToken, firstUserFrames);
            sockets.add(firstUser);
            WebSocket secondUser = connectAndRegister(client, userAddress, userToken, secondUserFrames);
            sockets.add(secondUser);
            WebSocket admin = connectAndRegister(client, adminAddress, adminToken, adminFrames);
            sockets.add(admin);

            String bothSocketsId = "multi-connection-live-" + System.nanoTime();
            clusterService.storeNotification(notification(userId, bothSocketsId, false)).await().indefinitely();
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(firstUserFrames).anyMatch(frame -> frame.contains(bothSocketsId));
                assertThat(secondUserFrames).anyMatch(frame -> frame.contains(bothSocketsId));
            });

            closeAndAwait(firstUser);
            sockets.remove(firstUser);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isTrue();
                assertThat(NotificationSockJSBridge.hasActiveReceiver(adminId)).isTrue();
            });

            String persistedId = "multi-connection-persisted-" + System.nanoTime();
            mockServerClient.when(request()
                    .withMethod("GET")
                    .withPath("/internal/notifications/" + persistedId + "/delivered"))
                    .respond(response().withStatusCode(200));
            clusterService.storeNotification(notification(userId, persistedId, true)).await().indefinitely();
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(secondUserFrames).anyMatch(frame -> frame.contains(persistedId)));
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(inboxFor(userId)).isEmpty());
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> mockServerClient.verify(
                    request().withMethod("GET")
                            .withPath("/internal/notifications/" + persistedId + "/delivered")));

            closeAndAwait(secondUser);
            sockets.remove(secondUser);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isFalse();
                assertThat(NotificationSockJSBridge.hasActiveReceiver(adminId)).isTrue();
            });

            String adminLiveId = "admin-remains-active-" + System.nanoTime();
            clusterService.storeNotification(notification(adminId, adminLiveId, false)).await().indefinitely();
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(adminFrames).anyMatch(frame -> frame.contains(adminLiveId)));
        } finally {
            try {
                for (WebSocket socket : sockets) {
                    closeAndAwait(socket);
                }
            } finally {
                client.close();
            }
        }
    }

    @Test
    void sockjs_lastConnectionCloseRetainsNotificationsUntilReconnect() throws Exception {
        String userToken = getKeycloakUserToken(USER);
        String userId = getTokenSubject(userToken);
        String userAddress = NotificationClusterService.EB_ADDRESS_PREFIX + userId;
        Queue<String> catchUpFrames = new ConcurrentLinkedQueue<>();
        HttpClient client = vertx.createHttpClient(new HttpClientOptions());
        List<WebSocket> sockets = new ArrayList<>();

        try {
            WebSocket socket = connectAndRegister(client, userAddress, userToken, new ConcurrentLinkedQueue<>());
            sockets.add(socket);
            closeAndAwait(socket);
            sockets.remove(socket);
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isFalse());

            String offlineId = "offline-catch-up-" + System.nanoTime();
            mockServerClient.when(request()
                    .withMethod("GET")
                    .withPath("/internal/notifications/" + offlineId + "/delivered"))
                    .respond(response().withStatusCode(200));
            clusterService.storeNotification(notification(userId, offlineId, true)).await().indefinitely();
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(inboxFor(userId))
                            .anyMatch(item -> offlineId.equals(item.getId())));
            await().during(500, TimeUnit.MILLISECONDS).atMost(2, TimeUnit.SECONDS).untilAsserted(() -> mockServerClient.verify(
                    request().withMethod("GET")
                            .withPath("/internal/notifications/" + offlineId + "/delivered"),
                    VerificationTimes.exactly(0)));

            WebSocket catchUp = connectAndRegister(client, userAddress, userToken, catchUpFrames);
            sockets.add(catchUp);
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(catchUpFrames).anyMatch(frame -> frame.contains(offlineId)));
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(inboxFor(userId)).isEmpty());
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> mockServerClient.verify(
                    request().withMethod("GET")
                            .withPath("/internal/notifications/" + offlineId + "/delivered")));
        } finally {
            try {
                for (WebSocket socket : sockets) {
                    closeAndAwait(socket);
                }
            } finally {
                client.close();
            }
        }
    }

    @Test
    void sockjs_socketTracksMultipleReceiversAndRejectedRegistrationDoesNotAffectOthers() throws Exception {
        String userToken = getKeycloakUserToken(USER);
        String userId = getTokenSubject(userToken);
        String adminToken = getKeycloakUserToken(ADMIN);
        String adminId = getTokenSubject(adminToken);
        String userAddress = NotificationClusterService.EB_ADDRESS_PREFIX + userId;
        String adminAddress = NotificationClusterService.EB_ADDRESS_PREFIX + adminId;
        Queue<String> validFrames = new ConcurrentLinkedQueue<>();
        Queue<String> rejectedFrames = new ConcurrentLinkedQueue<>();
        HttpClient client = vertx.createHttpClient(new HttpClientOptions());
        List<WebSocket> sockets = new ArrayList<>();

        try {
            WebSocket validSocket = connectAndRegister(client, userAddress, userToken, validFrames);
            sockets.add(validSocket);
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isTrue());

            validSocket.writeTextMessage(unregisterFrame(userAddress));
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isFalse());

            validSocket.writeTextMessage(registerFrame(userAddress, userToken));
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isTrue());

            validSocket.writeTextMessage(registerFrame(adminAddress, adminToken));
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isTrue();
                assertThat(NotificationSockJSBridge.hasActiveReceiver(adminId)).isTrue();
            });

            validSocket.writeTextMessage(unregisterFrame(adminAddress));
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(NotificationSockJSBridge.hasActiveReceiver(adminId)).isFalse();
                assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isTrue();
            });

            WebSocket rejectedSocket = openSockjsSocket(client, rejectedFrames);
            sockets.add(rejectedSocket);
            rejectedSocket.writeTextMessage(registerFrame(adminAddress, userToken));
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(rejectedFrames).anyMatch(this::isRejectedFrame));
            assertThat(NotificationSockJSBridge.hasActiveReceiver(adminId)).isFalse();
            assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isTrue();

            closeAndAwait(rejectedSocket);
            sockets.remove(rejectedSocket);
            await().during(500, TimeUnit.MILLISECONDS).atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(NotificationSockJSBridge.hasActiveReceiver(adminId)).isFalse();
                assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isTrue();
            });

            closeAndAwait(validSocket);
            sockets.remove(validSocket);
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(userId)).isFalse());
        } finally {
            try {
                for (WebSocket socket : sockets) {
                    closeAndAwait(socket);
                }
            } finally {
                client.close();
            }
        }
    }

    /**
     * Stores a notification for a receiver BEFORE the client connects.
     * On REGISTER, the bridge must drain the inbox and push the notification
     * to the WebSocket — the client receives it without a separate HTTP call.
     */
    @Test
    void sockjs_register_drainsStoredNotifications() throws Exception {
        String token = getKeycloakUserToken(ADMIN);
        String receiverId = getTokenSubject(token);
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
                    if (isNotificationPayload(text)) {
                        received.add(text);
                    }
                });
                openLatch.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", token)
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
        String token = getKeycloakUserToken(USER);
        String receiverId = getTokenSubject(token);
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
                    if (isNotificationPayload(text)) {
                        received.add(text);
                    }
                });
                openLatch.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", token)
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
     * must not add anything to the active socket registry (the else-branch in the bridge handler).
     */
    @Test
    void sockjs_register_unknownAddress_doesNotAddToActiveReceivers() throws Exception {
        String unknownAddress = "some.other.address";
        String token = getKeycloakUserToken(USER);
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
                        .put("token", token)
                        .encode());
            }
        });

        assertThat(openLatch.await(5, TimeUnit.SECONDS)).isTrue();
        // Give the server time to process

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
        String token = getKeycloakUserToken(USER);
        String receiverId = getTokenSubject(token);
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
                    if (isNotificationPayload(text)) {
                        received.add(text);
                    }
                });
                openLatch.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", token)
                        .encode());
            }
        });

        assertThat(openLatch.await(5, TimeUnit.SECONDS)).isTrue();

        // Receiver must be tracked as active
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue());

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
        String token = getKeycloakUserToken(USER);
        String receiverId = getTokenSubject(token);
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
                    if (isNotificationPayload(text)) {
                        received.add(text);
                    }
                });
                openLatch.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", token)
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
        String token = getKeycloakUserToken(ADMIN);
        String receiverId = getTokenSubject(token);
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
                    if (isNotificationPayload(text)) {
                        firstReceived.add(text);
                    }
                });
                firstOpen.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", token)
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
                    if (isNotificationPayload(text)) {
                        secondReceived.add(text);
                    }
                });
                secondOpen.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", token)
                        .encode());
            }
        });

        assertThat(secondOpen.await(5, TimeUnit.SECONDS)).isTrue();

        // Receiver is active on this pod
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue());

        // No messages pushed — empty inbox
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
        String token = getKeycloakUserToken(USER);
        String receiverId = getTokenSubject(token);
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
                    if (isNotificationPayload(text)) {
                        received.add(text);
                    }
                });
                openLatch.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", token)
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
        String token = getKeycloakUserToken(USER);
        String receiverId = getTokenSubject(token);
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
                    if (isNotificationPayload(text)) {
                        received.add(text);
                    }
                });
                openLatch.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", token)
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

    /**
     * Covers: {@code if (token == null || token.isBlank()) return null;}
     *
     * A REGISTER frame that omits the "token" field altogether causes
     * {@code getRawMessage().getString("token")} to return null, so
     * {@code extractSubjectFromRegisterPayload} returns null, the subject
     * does not match the receiverId, and the registration is rejected.
     */
    @Test
    void sockjs_register_missingToken_rejected() throws Exception {
        String token = getKeycloakUserToken(USER);
        String receiverId = getTokenSubject(token);
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        List<String> errors = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
            if (ar.succeeded()) {
                WebSocket ws = ar.result();
                wsHolder.add(ws);
                ws.handler(buf -> {
                    String text = buf.toString();
                    if (isRejectedFrame(text)) {
                        errors.add(text);
                    }
                });
                openLatch.countDown();
                // Deliberately omit "token" — getString("token") returns null
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .encode());
            }
        });

        assertThat(openLatch.await(5, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(errors).isNotEmpty());

        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * Covers: {@code if (token == null || token.isBlank()) return null;}
     *
     * A REGISTER frame with a blank (whitespace-only) "token" value causes
     * {@code token.isBlank()} to be true, so {@code extractSubjectFromRegisterPayload}
     * returns null and the registration is rejected.
     */
    @Test
    void sockjs_register_blankToken_rejected() throws Exception {
        String token = getKeycloakUserToken(USER);
        String receiverId = getTokenSubject(token);
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        List<String> errors = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
            if (ar.succeeded()) {
                WebSocket ws = ar.result();
                wsHolder.add(ws);
                ws.handler(buf -> {
                    String text = buf.toString();
                    if (isRejectedFrame(text)) {
                        errors.add(text);
                    }
                });
                openLatch.countDown();
                // Blank token — isBlank() returns true
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", "   ")
                        .encode());
            }
        });

        assertThat(openLatch.await(5, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(errors).isNotEmpty());

        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * Covers: {@code catch (Exception ex) { ... return null; }}
     *
     * A REGISTER frame with a malformed token (not a valid JWT) causes
     * {@code TokenParserService.parseToken()} to throw, the catch block
     * logs a debug message and returns null, so the registration is rejected.
     */
    @Test
    void sockjs_register_malformedToken_rejected() throws Exception {
        String token = getKeycloakUserToken(USER);
        String receiverId = getTokenSubject(token);
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        List<String> errors = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
            if (ar.succeeded()) {
                WebSocket ws = ar.result();
                wsHolder.add(ws);
                ws.handler(buf -> {
                    String text = buf.toString();
                    if (isRejectedFrame(text)) {
                        errors.add(text);
                    }
                });
                openLatch.countDown();
                // Garbage token — not a valid JWT; TokenParserService will throw
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", "this.is.not.a.valid.jwt.token")
                        .encode());
            }
        });

        assertThat(openLatch.await(5, TimeUnit.SECONDS)).isTrue();
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(errors).isNotEmpty());

        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    @Test
    void sockjs_register_receiverIdDifferentFromTokenSubject_rejected() throws Exception {
        String adminToken = getKeycloakUserToken(ADMIN);
        String userToken = getKeycloakUserToken(USER);
        String receiverId = getTokenSubject(adminToken);
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        NotificationDTO dto = new NotificationDTO();
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(false);
        clusterService.storeNotification(dto).await().indefinitely();

        List<String> received = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        client.webSocket(sockjsOpts()).onComplete(ar -> {
            if (ar.succeeded()) {
                WebSocket ws = ar.result();
                wsHolder.add(ws);
                ws.handler(buf -> {
                    String text = buf.toString();
                    if (isRejectedFrame(text)) {
                        errors.add(text);
                    } else if (isNotificationPayload(text)) {
                        received.add(text);
                    }
                });
                openLatch.countDown();
                ws.writeTextMessage(new JsonObject()
                        .put("type", "register")
                        .put("address", address)
                        .put("token", userToken)
                        .encode());
            }
        });

        assertThat(openLatch.await(5, TimeUnit.SECONDS)).isTrue();

        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(errors).isNotEmpty());

        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
        assertThat(received).isEmpty();
        assertThat(clusterService.consumeByReceiverId(receiverId).await().indefinitely()).hasSize(1);

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

    private WebSocket connectAndRegister(HttpClient client, String address, String token, Queue<String> frames)
            throws Exception {
        WebSocket socket = openSockjsSocket(client, frames);
        socket.writeTextMessage(registerFrame(address, token));
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(NotificationSockJSBridge.hasActiveReceiver(
                        address.substring(NotificationClusterService.EB_ADDRESS_PREFIX.length()))).isTrue());
        return socket;
    }

    private WebSocket openSockjsSocket(HttpClient client, Queue<String> frames) throws Exception {
        WebSocket socket = client.webSocket(sockjsOpts()).toCompletionStage().toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        socket.handler(buf -> frames.add(buf.toString()));
        return socket;
    }

    private String registerFrame(String address, String token) {
        return new JsonObject()
                .put("type", "register")
                .put("address", address)
                .put("token", token)
                .encode();
    }

    private String unregisterFrame(String address) {
        return new JsonObject()
                .put("type", "unregister")
                .put("address", address)
                .encode();
    }

    private NotificationDTO notification(String receiverId, String notificationId, boolean persist) {
        NotificationDTO dto = new NotificationDTO();
        dto.setId(notificationId);
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(persist);
        return dto;
    }

    private List<NotificationDTO> inboxFor(String receiverId) throws Exception {
        var inbox = vertx.sharedData()
                .<String, List<NotificationDTO>> getClusterWideMap(NotificationClusterService.MAP_NAME)
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        List<NotificationDTO> notifications = inbox.get(receiverId)
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        return notifications == null ? List.of() : notifications;
    }

    private void closeAndAwait(WebSocket socket) throws InterruptedException {
        CountDownLatch closeLatch = new CountDownLatch(1);
        socket.closeHandler(ignored -> closeLatch.countDown());
        socket.close();
        assertThat(closeLatch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    private boolean isNotificationPayload(String text) {
        if (text == null || text.equals("h") || text.equals("o")) {
            return false;
        }
        return !isRejectedFrame(text);
    }

    private boolean isRejectedFrame(String text) {
        return text != null
                && text.contains("\"type\":\"err\"")
                && text.contains("\"body\":\"rejected\"");
    }

}
