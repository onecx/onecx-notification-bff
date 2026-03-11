package org.tkit.onecx.notification.bff.rs.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.tkit.onecx.notification.bff.rs.ws.NotificationSockJSBridge;
import org.tkit.quarkus.log.cdi.LogService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationRetrieveRequestDTO;
import gen.org.tkit.onecx.notification.svc.internal.client.api.NotificationInternalApi;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.EventBus;
import io.vertx.mutiny.core.shareddata.AsyncMap;

/**
 * Central cluster-wide service that owns all notification storage and delivery logic.
 *
 * Architecture overview:
 *
 *   REST /dispatch
 *       │
 *       ▼
 *   storeNotification()
 *       ├─► Hazelcast IMap  (keyed by receiverId — persists across pod restarts)
 *       └─► Hazelcast ITopic  (cluster broadcast — ALL pods receive every publish)
 *                │
 *                ▼  (on every pod, including the one that published)
 *           topic listener
 *               ├─► Vert.x EventBus.publish(localAddress)
 *               │       └─► SockJS bridge forwards to connected browser
 *               └─► if receiver is currently connected on THIS pod:
 *                       ├─► remove entry from IMap  (prevent re-delivery on reconnect)
 *                       └─► if persist=true: markNotificationAsDelivered() on SVC
 *
 * Offline / missed-message handling:
 * When a receiver is not connected at delivery time, the notification stays in
 * the Hazelcast IMap. On the next SockJS REGISTER event (handled by
 * {@link NotificationSockJSBridge}), {@link #consumeByReceiverId(String)} drains the
 * inbox and pushes each stored notification directly to the client.
 *
 * Key constants:
 *   - {@link #MAP_NAME} — name of the cluster-wide Hazelcast IMap used as the inbox store
 *   - {@link #EB_ADDRESS_PREFIX} — prefix of the local Vert.x EventBus addresses;
 *       full address is {@code EB_ADDRESS_PREFIX + receiverId}
 *   - {@code TOPIC_NAME} — name of the Hazelcast ITopic used for cross-pod fan-out
 */
@ApplicationScoped
@LogService
public class NotificationClusterService {

    /** Name of the Hazelcast cluster-wide map used as the per-receiver inbox store. */
    public static final String MAP_NAME = "notifications";

    /**
     * Prefix for local Vert.x EventBus addresses.
     * The full address for a receiver is {@code EB_ADDRESS_PREFIX + receiverId},
     * e.g. {@code "notifications.onecx.new.receiver1"}.
     * The SockJS bridge exposes this namespace to connected browsers.
     */
    public static final String EB_ADDRESS_PREFIX = "notifications.onecx.new.";

    /** Name of the Hazelcast ITopic used to broadcast notifications to every pod in the cluster. */
    private static final String TOPIC_NAME = "notifications.onecx.topic";

    private static final Logger LOG = Logger.getLogger(NotificationClusterService.class);

    @Inject
    Vertx vertx;

    @Inject
    EventBus eventBus;

    /** Used to serialize {@link NotificationDTO} to JSON for the Hazelcast topic payload. */
    @Inject
    ObjectMapper objectMapper;

    /** MicroProfile REST client for calling the downstream notification SVC (mark-as-delivered). */
    @Inject
    @RestClient
    NotificationInternalApi notificationSVCClient;

    /**
     * Lazily resolved and cached reference to the Hazelcast cluster-wide IMap.
     * The map is resolved once on first use via {@link #map()} and then reused.
     */
    private final AtomicReference<AsyncMap<String, List<NotificationDTO>>> clusterMapRef = new AtomicReference<>();

    /**
     * Hazelcast reliable topic used for cluster-wide pub/sub.
     * Initialized in {@link #onStart(StartupEvent)}.
     */
    private ITopic<String> topic;

    /**
     * Initializes the Hazelcast topic listener at application startup.
     *
     * Retrieves the Hazelcast instance (registered globally by the vertx-hazelcast integration)
     * and subscribes a MessageListener to {@code TOPIC_NAME}.
     *
     * The listener runs on a Hazelcast thread (not a Vert.x thread). For every received
     * topic message it:
     *   1. Deserializes the JSON payload back into a {@link NotificationDTO}.
     *   2. Switches onto the Vert.x event loop via {@code vertx.runOnContext()} so that
     *      the EventBus publish is thread-safe.
     *   3. Publishes the raw JSON string to the local Vert.x EventBus address
     *      ({@link #EB_ADDRESS_PREFIX} + receiverId) so the SockJS bridge can
     *      forward it to any browser session registered on that address on this pod.
     *   4. If {@link NotificationSockJSBridge#hasActiveReceiver(String)} returns {@code true}
     *      (i.e. the receiver has an open SockJS session on this pod):
     *        - Removes the notification list from the IMap to prevent re-delivery on reconnect.
     *        - If {@code persist=true}, calls {@code markNotificationAsDelivered(id)} on the SVC.
     */
    void onStart(@Observes StartupEvent ev) {
        // Hazelcast registers its instance globally — retrieve it without any casting or CDI tricks
        HazelcastInstance hz = Hazelcast.getAllHazelcastInstances().iterator().next();
        topic = hz.getTopic(TOPIC_NAME);
        topic.addMessageListener(message -> {
            String json = message.getMessageObject();
            NotificationDTO notification;
            try {
                notification = objectMapper.readValue(json, NotificationDTO.class);
            } catch (Exception e) {
                LOG.errorf("Failed to deserialize topic message: %s", e.getMessage());
                return;
            }
            String receiverId = notification.getReceiverId();
            if (receiverId != null) {
                String localAddress = EB_ADDRESS_PREFIX + receiverId;
                LOG.debugf("Hazelcast topic → local EventBus publish to '%s'", localAddress);
                vertx.runOnContext(() -> {
                    eventBus.publish(localAddress, json);
                    if (NotificationSockJSBridge.hasActiveReceiver(receiverId)) {
                        Boolean persist = notification.getPersist();
                        String notificationId = notification.getId();
                        map().flatMap(m -> m.remove(receiverId))
                                .subscribe().with(
                                        ignored -> {
                                            LOG.infof(
                                                    "Removed live-delivered notification from IMap key='%s'",
                                                    receiverId);
                                            if (Boolean.TRUE.equals(persist) && notificationId != null) {
                                                try (var r = notificationSVCClient
                                                        .markNotificationAsDelivered(notificationId)) {
                                                    LOG.infof("Marked notification id='%s' as delivered (status=%d)",
                                                            notificationId, r.getStatus());
                                                } catch (Exception ex) {
                                                    LOG.warnf("Failed to mark notification id='%s' as delivered: %s",
                                                            notificationId, ex.getMessage());
                                                }
                                            }
                                        },
                                        err -> LOG.warnf(
                                                "Failed to remove live-delivered notification: %s",
                                                err.getMessage()));
                    }
                });
            }
        });

        LOG.info("Hazelcast topic listener registered on '" + TOPIC_NAME + "'");
    }

    /**
     * Lazily resolves the Hazelcast cluster-wide {@link AsyncMap} and caches it in
     * {@link #clusterMapRef} for all subsequent calls.
     *
     * The map is shared across the entire Hazelcast cluster, so every pod reads
     * and writes to the same data structure regardless of which pod received the
     * original REST request.
     *
     * @return a {@link Uni} that emits the shared {@link AsyncMap}
     */
    private Uni<AsyncMap<String, List<NotificationDTO>>> map() {
        AsyncMap<String, List<NotificationDTO>> cached = clusterMapRef.get();
        if (cached != null) {
            return Uni.createFrom().item(cached);
        }
        return vertx.sharedData()
                .<String, List<NotificationDTO>> getClusterWideMap(MAP_NAME)
                .invoke(m -> {
                    clusterMapRef.compareAndSet(null, m);
                    LOG.info("Cluster-wide map '" + MAP_NAME + "' resolved and cached");
                });
    }

    /**
     * Stores a notification in the cluster-wide IMap and broadcasts it to all pods.
     *
     * Steps performed:
     *   1. Resolves the cluster-wide {@link AsyncMap} keyed by {@code receiverId}.
     *   2. Fetches the existing list for that receiver (or starts a new empty list).
     *   3. Appends the new notification and writes the updated list back to the IMap.
     *      This ensures offline receivers can still retrieve the notification later.
     *   4. Serializes the full {@link NotificationDTO} to JSON via {@link ObjectMapper}
     *      and publishes it to the Hazelcast ITopic. Because the topic is a true
     *      pub/sub, every pod in the cluster (including this one) will receive
     *      the message in their registered MessageListener.
     *
     * The IMap key is {@code receiverId} only — {@code applicationId} is part of the
     * DTO payload and is used by the frontend for display purposes, not for routing.
     *
     * @param notification the notification to store and broadcast
     * @return a {@link Uni} that completes when the IMap write and topic publish are done
     */
    public Uni<Void> storeNotification(NotificationDTO notification) {
        // Key is simply the receiverId — applicationId is part of the payload, not the key
        String key = notification.getReceiverId();

        return map().flatMap(m -> m.get(key)
                .map(existing -> existing != null ? existing : new ArrayList<NotificationDTO>())
                .flatMap(list -> {
                    list.add(notification);
                    return m.put(key, list);
                })
                .flatMap(ignored -> {
                    try {
                        String json = objectMapper.writeValueAsString(notification);
                        // Publish to Hazelcast ITopic — true broadcast, ALL pods receive it
                        topic.publish(json);
                        LOG.debugf("Published to Hazelcast topic for receiverId='%s'",
                                notification.getReceiverId());
                    } catch (Exception e) {
                        LOG.errorf("Failed to serialize notification for topic: %s", e.getMessage());
                    }
                    return Uni.createFrom().voidItem();
                }));
    }

    /**
     * Drains the inbox for the given receiver in a single atomic remove-and-return operation.
     *
     * This is the "missed message" drain used by {@link NotificationSockJSBridge} on every
     * SockJS REGISTER event. Because the entry is removed from the IMap atomically,
     * the same notification list will not be returned a second time — preventing duplicate
     * delivery across reconnects.
     *
     * Notifications with {@code persist=true} that are drained here are subsequently
     * marked as delivered via the SVC client in {@link NotificationSockJSBridge}.
     *
     * @param receiverId the receiver whose inbox should be drained; blank or {@code null} returns empty
     * @return a {@link Uni} that emits the removed notification list, or an empty list if none exist
     */
    public Uni<List<NotificationDTO>> consumeByReceiverId(String receiverId) {
        if (receiverId == null || receiverId.isBlank()) {
            return Uni.createFrom().item(List.of());
        }
        // Key is simply receiverId — remove and return in one step
        return map().flatMap(m -> m.remove(receiverId)
                .map(list -> list != null ? list : List.<NotificationDTO> of()));
    }
}
