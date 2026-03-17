package org.tkit.onecx.notification.bff.rs.service;

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
import org.tkit.onecx.notification.bff.rs.ws.NotificationSockJSBridge;

import com.hazelcast.core.Hazelcast;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.ContentMetaDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.IssuerDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.SeverityDTO;
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
class NotificationClusterServiceTest extends AbstractTest {

    @Inject
    NotificationClusterService service;

    @Inject
    Vertx vertx;

    @InjectMockServerClient
    MockServerClient mockServerClient;

    @TestHTTPResource("/")
    URL baseUrl;

    /** Drain all receiverIds used in this test class before each test. */
    @BeforeEach
    void drainAll() {
        for (String r : List.of("r1", "r2", "r3", "persist-receiver", "meta-receiver", "live-receiver",
                "live-persist-receiver", "live-persist-fail-receiver", "live-persist-null-id-receiver")) {
            service.consumeByReceiverId(r).await().indefinitely();
        }
    }

    // -------------------------------------------------------------------------
    // storeNotification — verify notification ends up in the map
    // -------------------------------------------------------------------------

    @Test
    void store_singleNotification_canBeConsumed() {
        NotificationDTO dto = notification("r1", false);

        service.storeNotification(dto).await().indefinitely();

        List<NotificationDTO> result = service.consumeByReceiverId("r1").await().indefinitely();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getReceiverId()).isEqualTo("r1");
    }

    @Test
    void store_preservesAllScalarFields() {
        NotificationDTO dto = notification("r1", true);
        dto.setId("id-42");
        dto.setApplicationId("app-x");
        dto.setSenderId("sender-x");
        dto.setSeverity(SeverityDTO.CRITICAL);
        dto.setIssuer(IssuerDTO.SYSTEM);

        service.storeNotification(dto).await().indefinitely();

        NotificationDTO stored = service.consumeByReceiverId("r1").await().indefinitely().get(0);
        assertThat(stored.getId()).isEqualTo("id-42");
        assertThat(stored.getApplicationId()).isEqualTo("app-x");
        assertThat(stored.getSenderId()).isEqualTo("sender-x");
        assertThat(stored.getPersist()).isTrue();
        assertThat(stored.getSeverity()).isEqualTo(SeverityDTO.CRITICAL);
        assertThat(stored.getIssuer()).isEqualTo(IssuerDTO.SYSTEM);
    }

    @Test
    void store_preservesContentMeta() {
        NotificationDTO dto = notification("meta-receiver", false);
        ContentMetaDTO meta = new ContentMetaDTO();
        meta.setKey("k");
        meta.setValue("v");
        dto.setContentMeta(List.of(meta));

        service.storeNotification(dto).await().indefinitely();

        NotificationDTO stored = service.consumeByReceiverId("meta-receiver")
                .await().indefinitely().get(0);
        assertThat(stored.getContentMeta()).hasSize(1);
        assertThat(stored.getContentMeta().get(0).getKey()).isEqualTo("k");
        assertThat(stored.getContentMeta().get(0).getValue()).isEqualTo("v");
    }

    @Test
    void store_multipleNotifications_appendedToSameKey() {
        service.storeNotification(notification("r2", false)).await().indefinitely();
        service.storeNotification(notification("r2", false)).await().indefinitely();
        service.storeNotification(notification("r2", false)).await().indefinitely();

        List<NotificationDTO> result = service.consumeByReceiverId("r2").await().indefinitely();
        assertThat(result).hasSize(3);
    }

    @Test
    void store_differentReceivers_storedUnderSeparateKeys() {
        service.storeNotification(notification("r1", false)).await().indefinitely();
        service.storeNotification(notification("r2", false)).await().indefinitely();

        assertThat(service.consumeByReceiverId("r1").await().indefinitely()).hasSize(1);
        assertThat(service.consumeByReceiverId("r2").await().indefinitely()).hasSize(1);
    }

    // -------------------------------------------------------------------------
    // consumeByReceiverId — remove-and-return semantics
    // -------------------------------------------------------------------------

    @Test
    void consume_emptyInbox_returnsEmptyList() {
        List<NotificationDTO> result = service.consumeByReceiverId("r3").await().indefinitely();
        assertThat(result).isEmpty();
    }

    @Test
    void consume_removesEntryAtomically() {
        service.storeNotification(notification("r1", false)).await().indefinitely();

        // First call drains and removes
        List<NotificationDTO> first = service.consumeByReceiverId("r1").await().indefinitely();
        assertThat(first).hasSize(1);

        // Second call returns empty — no duplicate delivery
        List<NotificationDTO> second = service.consumeByReceiverId("r1").await().indefinitely();
        assertThat(second).isEmpty();
    }

    @Test
    void consume_nullReceiverId_returnsEmptyListWithoutException() {
        List<NotificationDTO> result = service.consumeByReceiverId(null).await().indefinitely();
        assertThat(result).isEmpty();
    }

    @Test
    void consume_blankReceiverId_returnsEmptyListWithoutException() {
        List<NotificationDTO> result = service.consumeByReceiverId("  ").await().indefinitely();
        assertThat(result).isEmpty();
    }

    @Test
    void consume_emptyStringReceiverId_returnsEmptyListWithoutException() {
        List<NotificationDTO> result = service.consumeByReceiverId("").await().indefinitely();
        assertThat(result).isEmpty();
    }

    @Test
    void consume_doesNotAffectOtherReceivers() {
        service.storeNotification(notification("r1", false)).await().indefinitely();
        service.storeNotification(notification("r2", false)).await().indefinitely();

        // Consume only r1
        service.consumeByReceiverId("r1").await().indefinitely();

        // r2 inbox must still be intact
        assertThat(service.consumeByReceiverId("r2").await().indefinitely()).hasSize(1);
    }

    // -------------------------------------------------------------------------
    // Address constants — used by the bridge, verified here for correctness
    // -------------------------------------------------------------------------

    @Test
    void constants_mapName_isExpectedValue() {
        assertThat(NotificationClusterService.MAP_NAME).isEqualTo("notifications");
    }

    @Test
    void constants_ebAddressPrefix_endsWithDot() {
        // The bridge appends receiverId directly — prefix must end with a dot
        assertThat(NotificationClusterService.EB_ADDRESS_PREFIX).endsWith(".");
    }

    @Test
    void constants_ebAddress_forReceiver_isCorrect() {
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + "receiver1";
        assertThat(address).isEqualTo("notifications.onecx.new.receiver1");
    }

    @Test
    void topicListener_hasActiveReceiver_true_iMapEntryRemovedAfterLiveDelivery() throws Exception {
        String receiverId = "live-receiver";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        List<String> received = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        // Connect and REGISTER so hasActiveReceiver() returns true for this receiverId
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

        // Now store — the topic listener fires with hasActiveReceiver=true
        NotificationDTO dto = new NotificationDTO();
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(false);
        service.storeNotification(dto).await().indefinitely();

        // Notification must arrive on the WebSocket (live delivery via EventBus)
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isNotEmpty());
        assertThat(received.get(0)).contains(receiverId);

        // IMap entry must have been removed by the topic listener (live delivery clears it)
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            List<NotificationDTO> stored = service.consumeByReceiverId(receiverId).await().indefinitely();
            assertThat(stored).isEmpty();
        });

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * Publishes a JSON message directly to the Hazelcast topic with {@code "receiverId": null}.
     * The listener deserializes it, finds receiverId is null, and skips the entire
     * EventBus-publish block. No exception must be thrown and the application must remain stable.
     */
    @Test
    void topicListener_nullReceiverId_skipsEventBusPublish() throws Exception {
        // Publish a topic message with receiverId=null directly — bypasses storeNotification
        String json = "{\"applicationId\":\"app1\",\"senderId\":\"sender\",\"receiverId\":null,\"persist\":false}";
        Hazelcast.getAllHazelcastInstances().iterator().next()
                .<String> getTopic(NotificationClusterService.TOPIC_NAME)
                .publish(json);

        // Wait a moment — no exception must surface and the application must remain healthy
        Thread.sleep(500);

        // No side-effects: nothing was added to any receiver's inbox
        assertThat(service.consumeByReceiverId("r1").await().indefinitely()).isEmpty();
    }

    /**
     * When persist=true but the notification id is null, the right-hand side of {@code &&}
     * is false so the {@code executeBlocking} call is skipped. The notification must still
     * be live-delivered to the WebSocket and the IMap entry must be removed.
     * No call to markNotificationAsDelivered must be made.
     */
    @Test
    void topicListener_hasActiveReceiver_persistTrue_nullId_markAsDeliveredSkipped() throws Exception {
        String receiverId = "live-persist-null-id-receiver";
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

        // persist=true but id=null → Boolean.TRUE.equals(true) && null != null → false → skip executeBlocking
        NotificationDTO dto = new NotificationDTO();
        dto.setId(null);
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(true);
        service.storeNotification(dto).await().indefinitely();

        // Notification must still arrive (live delivery is unaffected by the id=null branch)
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isNotEmpty());
        assertThat(received.get(0)).contains(receiverId);

        // IMap entry must have been removed
        await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(service.consumeByReceiverId(receiverId).await().indefinitely()).isEmpty());

        // markNotificationAsDelivered must NOT have been called (id was null)
        mockServerClient.verifyZeroInteractions();

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * A receiver is connected and active. A notification with persist=true and a non-null
     * id is stored. The topic listener fires, detects hasActiveReceiver=true, removes
     * the IMap entry, then calls markNotificationAsDelivered via executeBlocking.
     */
    @Test
    void topicListener_hasActiveReceiver_persistTrue_markAsDeliveredSucceeds() throws Exception {
        String receiverId = "live-persist-receiver";
        String notifId = "live-persist-id-1";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        mockServerClient
                .when(request()
                        .withMethod("GET")
                        .withPath("/internal/notifications/" + notifId + "/delivered"))
                .respond(response().withStatusCode(200));

        List<String> received = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        // Connect and REGISTER so hasActiveReceiver() returns true for this receiverId
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

        // Store with persist=true and a real id
        NotificationDTO dto = new NotificationDTO();
        dto.setId(notifId);
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(true);
        service.storeNotification(dto).await().indefinitely();

        // Notification must arrive on the WebSocket (live delivery)
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isNotEmpty());
        assertThat(received.get(0)).contains(receiverId);

        // IMap entry must have been removed
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            List<NotificationDTO> stored = service.consumeByReceiverId(receiverId).await().indefinitely();
            assertThat(stored).isEmpty();
        });

        // markNotificationAsDelivered must have been called and succeeded
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> mockServerClient.verify(
                request()
                        .withMethod("GET")
                        .withPath("/internal/notifications/" + notifId + "/delivered")));

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    /**
     * A receiver is connected and active. A notification with persist=true and a non-null
     * id is stored. The topic listener fires, detects hasActiveReceiver=true, removes
     * the IMap entry, then calls markNotificationAsDelivered via executeBlocking.
     * Notification delivery is unaffected — it still arrives on the WebSocket.
     */
    @Test
    void topicListener_hasActiveReceiver_persistTrue_markAsDeliveredFails_loggedAndNotPropagated() throws Exception {
        String receiverId = "live-persist-fail-receiver";
        String notifId = "live-persist-fail-id-1";
        String address = NotificationClusterService.EB_ADDRESS_PREFIX + receiverId;

        mockServerClient
                .when(request()
                        .withMethod("GET")
                        .withPath("/internal/notifications/" + notifId + "/delivered"))
                .respond(response().withStatusCode(500));

        List<String> received = new ArrayList<>();
        CountDownLatch openLatch = new CountDownLatch(1);
        List<WebSocket> wsHolder = new ArrayList<>();

        HttpClient client = vertx.createHttpClient(new HttpClientOptions());

        // Connect and REGISTER so hasActiveReceiver() returns true for this receiverId
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

        // Store with persist=true and a real id
        NotificationDTO dto = new NotificationDTO();
        dto.setId(notifId);
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(true);
        service.storeNotification(dto).await().indefinitely();

        // Notification must still arrive on the WebSocket despite mark-as-delivered failure
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(received).isNotEmpty());
        assertThat(received.get(0)).contains(receiverId);

        // IMap entry must have been removed (the remove happens before markAsDelivered)
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            List<NotificationDTO> stored = service.consumeByReceiverId(receiverId).await().indefinitely();
            assertThat(stored).isEmpty();
        });

        // markNotificationAsDelivered was called
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> mockServerClient.verify(
                request()
                        .withMethod("GET")
                        .withPath("/internal/notifications/" + notifId + "/delivered")));

        if (!wsHolder.isEmpty())
            wsHolder.get(0).close();
        client.close();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private NotificationDTO notification(String receiverId, boolean persist) {
        NotificationDTO dto = new NotificationDTO();
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(persist);
        return dto;
    }
}
