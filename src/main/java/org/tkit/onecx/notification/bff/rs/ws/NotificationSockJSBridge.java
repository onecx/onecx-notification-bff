package org.tkit.onecx.notification.bff.rs.ws;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.tkit.onecx.notification.bff.rs.config.NotificationConfig;
import org.tkit.onecx.notification.bff.rs.service.NotificationClusterService;
import org.tkit.quarkus.log.cdi.LogService;
import org.tkit.quarkus.rs.context.token.TokenParserRequest;
import org.tkit.quarkus.rs.context.token.TokenParserService;

import com.fasterxml.jackson.databind.ObjectMapper;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import gen.org.tkit.onecx.notification.svc.internal.client.api.NotificationInternalApi;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.BridgeEvent;
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;

/**
 * Mounts the SockJS event bus bridge and handles the full browser-facing
 * WebSocket lifecycle for real-time notification delivery.
 *
 * How the bridge works:
 * The SockJS event bus bridge exposes the internal event bus to browsers
 * over a SockJS connection (HTTP long-poll / WebSocket upgrade). Browsers connect to
 * {@code /eventbus/*} and communicate using the SockJS EventBus protocol (JSON frames).
 *
 * <h2>Receivers, sockets, and registrations</h2>
 * <ul>
 * <li>A <b>receiver</b> is the logical notification recipient, identified by
 * {@code receiverId}. Its event bus address is {@code notifications.onecx.new.<receiverId>}.
 * A receiver is not a network connection.</li>
 * <li>A <b>socket</b> is one SockJS connection, identified here by {@code writeHandlerID()}.
 * For example, two browser tabs can open separate sockets for the same receiver.</li>
 * <li>A <b>registration</b> is a socket's subscription request for one receiver's address.
 * It is pending after an authorized REGISTER and active only after the matching REGISTERED.
 * An open socket alone does not make a receiver active.</li>
 * </ul>
 * <p>The registry tracks at most one registration per socket/receiver pair. A socket can
 * have registrations for multiple receivers (each separately authorized), and a receiver
 * can have registrations on multiple sockets. For example, two registered tabs for the
 * same receiver mean one receiver, two sockets, and two active registrations. Closing
 * one tab removes only its registration; the receiver stays active through the other tab.
 * All registration state described here is local to this pod.</p>
 *
 * Connection lifecycle:
 * 1. SOCKET_CREATED — a browser has opened the SockJS connection. Logged only.
 * 2. REGISTER — the browser requests an EventBus address of the form
 * {@code notifications.onecx.new.<receiverId>}. At this point:
 * - The JWT token must be present in the JSON payload as the {@code "token"} field.
 * - The token subject ({@code sub}) is extracted and compared to the {@code receiverId}.
 * - If they do not match, the registration is rejected.
 * - Otherwise, the registration request is tracked as pending.
 * 3. REGISTERED — after the SockJS bridge accepts the registration, the matching pending request
 * becomes active and the IMap inbox is drained via
 * {@link NotificationClusterService#consumeByReceiverId(String)} and each
 * stored notification is immediately published to the local EventBus so the
 * bridge delivers it to the client — no separate HTTP call needed.
 * - Drained notifications with {@code persist=true} are marked as delivered
 * via the downstream SVC REST client.
 * 4. UNREGISTER — cancels the socket's pending or active registration for that address.
 * 5. SOCKET_CLOSED / SOCKET_ERROR — cancels all registrations for that socket.
 * Late REGISTERED events for cancelled or superseded requests are ignored.
 *
 * Security:
 * - The initial SockJS HTTP/WebSocket handshake is open ({@code policy=permit}) for browser
 * compatibility. Browsers cannot send custom headers on handshake due to Same-Origin Policy.
 * - Authorization is enforced at REGISTER time by validating the JWT token in the payload.
 * - Only outbound addresses ({@code notifications.onecx.new.*}) are permitted.
 * - No inbound permissions are configured, so browsers cannot publish to the EventBus —
 * all writes go through the REST API ({@code POST /notifications/dispatch}).
 * - On REGISTER, the bridge performs resource-level authorization: token subject ({@code sub})
 * must equal the requested {@code receiverId}.
 * - The token is expected to be present in every REGISTER message. If missing or invalid,
 * registration is rejected.
 */
@ApplicationScoped
@LogService
public class NotificationSockJSBridge {

    @Inject
    NotificationConfig notificationConfig;

    private static final Logger LOG = Logger.getLogger(NotificationSockJSBridge.class);

    /** The route path on which the SockJS EventBus bridge is mounted. */
    public static final String BRIDGE_PATH = "/eventbus/*";

    /**
     * Pod-local mapping of SockJS socket IDs to pending or active registration requests.
     * Shape: {@code socketId -> (receiverId -> original REGISTER message)}.
     * Entries remain here after activation; membership in {@link #ACTIVE_RECEIVERS}
     * distinguishes active registrations from pending ones. The original message object
     * identifies the request whose REGISTERED confirmation is allowed to activate it.
     *
     * The two indexes are updated together under {@link #ACTIVE_SOCKETS_LOCK}.
     * The Hazelcast topic listener in {@link NotificationClusterService} reads
     * {@code ACTIVE_RECEIVERS} from a Hazelcast listener thread.
     *
     * The registry is deliberately pod-local: each pod independently tracks its own
     * connected sockets, which is all that is needed to decide whether to clear the
     * IMap entry after live delivery on that pod.
     */
    private static final Map<String, Map<String, JsonObject>> SOCKET_REGISTRATIONS = new HashMap<>();

    /**
     * Reverse index of completed registrations: {@code receiverId -> active socket IDs}.
     * Pending registrations are excluded. A receiver key exists only while at least
     * one socket has an active registration for it.
     */
    private static final ConcurrentHashMap<String, Set<String>> ACTIVE_RECEIVERS = new ConcurrentHashMap<>();

    private static final Object ACTIVE_SOCKETS_LOCK = new Object();

    /**
     * Returns {@code true} if the given {@code receiverId} has at least one active
     * registration on this pod. Open sockets with only pending requests do not count.
     *
     * Called by {@link NotificationClusterService}'s topic listener to decide whether
     * to remove the IMap entry after publishing to the local EventBus (live delivery path).
     * If no active session exists, the IMap entry is left intact so the notification can
     * be drained after the client's next registration is confirmed with REGISTERED.
     *
     * @param receiverId the receiver to check
     * @return {@code true} if the receiver has an active registration on this pod
     */
    public static boolean hasActiveReceiver(String receiverId) {
        return receiverId != null && ACTIVE_RECEIVERS.containsKey(receiverId);
    }

    /**
     * Counts sockets with an active registration for this receiver on this pod.
     * Pending requests and duplicate confirmations do not increase the count.
     * Used to synchronize connection lifecycle tests.
     */
    static int activeConnectionCount(String receiverId) {
        synchronized (ACTIVE_SOCKETS_LOCK) {
            Set<String> socketIds = ACTIVE_RECEIVERS.get(receiverId);
            return socketIds == null ? 0 : socketIds.size();
        }
    }

    /**
     * Records an authorized REGISTER as pending without making the receiver active.
     * An existing pending or active request for this socket/receiver pair is preserved,
     * so duplicate REGISTER events cannot replace the request awaiting confirmation.
     *
     * @param message original REGISTER message, retained to match its later REGISTERED event
     */
    private static void trackRegistration(String socketId, String receiverId, JsonObject message) {
        synchronized (ACTIVE_SOCKETS_LOCK) {
            SOCKET_REGISTRATIONS.computeIfAbsent(socketId, ignored -> new HashMap<>())
                    .putIfAbsent(receiverId, message);
        }
    }

    /**
     * Promotes the tracked request when its matching REGISTERED event arrives.
     * The confirmation must carry the same message object as the tracked REGISTER;
     * equal payloads alone cannot distinguish an old cancelled request from a new one.
     *
     * @return {@code true} only on the first activation, allowing the caller to drain the inbox;
     *         {@code false} for duplicate confirmations or requests that were removed or replaced
     */
    private static boolean activateRegistration(String socketId, String receiverId, JsonObject message) {
        synchronized (ACTIVE_SOCKETS_LOCK) {
            Map<String, JsonObject> registrations = SOCKET_REGISTRATIONS.get(socketId);
            if (registrations == null || registrations.get(receiverId) != message) {
                return false;
            }
            return ACTIVE_RECEIVERS.computeIfAbsent(receiverId, ignored -> new HashSet<>()).add(socketId);
        }
    }

    /**
     * Handles UNREGISTER by removing one socket/receiver pair from both indexes,
     * whether pending or active. Other receivers on this socket and other sockets
     * for this receiver remain unchanged. Repeated removal has no effect.
     */
    private static void removeRegistration(String socketId, String receiverId) {
        synchronized (ACTIVE_SOCKETS_LOCK) {
            Map<String, JsonObject> registrations = SOCKET_REGISTRATIONS.get(socketId);
            if (registrations == null) {
                return;
            }
            registrations.remove(receiverId);
            if (registrations.isEmpty()) {
                SOCKET_REGISTRATIONS.remove(socketId);
            }
            removeActiveReceiver(socketId, receiverId);
        }
    }

    /**
     * Handles socket close or error by removing all pending and active registrations
     * belonging to that socket. Registrations on other sockets are preserved.
     * Repeated cleanup has no effect, and late confirmations cannot reactivate removed requests.
     */
    private static void removeSocket(String socketId) {
        synchronized (ACTIVE_SOCKETS_LOCK) {
            Map<String, JsonObject> registrations = SOCKET_REGISTRATIONS.remove(socketId);
            if (registrations == null) {
                return;
            }
            for (String receiverId : registrations.keySet()) {
                removeActiveReceiver(socketId, receiverId);
            }
        }
    }

    /**
     * Removes only this socket from the receiver's active-socket index, not the receiver
     * itself or its inbox. Drops the receiver key when its last active socket is removed.
     * Does not modify {@link #SOCKET_REGISTRATIONS}; callers update that index separately
     * and must hold {@link #ACTIVE_SOCKETS_LOCK}.
     */
    private static void removeActiveReceiver(String socketId, String receiverId) {
        ACTIVE_RECEIVERS.computeIfPresent(receiverId, (ignored, socketIds) -> {
            socketIds.remove(socketId);
            return socketIds.isEmpty() ? null : socketIds;
        });
    }

    /**
     * Extracts the receiver suffix from a notification event bus address, or returns
     * {@code null} for a missing address or another namespace. This does not authorize
     * the receiver or validate that the suffix is non-empty.
     */
    private static String receiverIdFromAddress(String address) {
        if (address == null || !address.startsWith(NotificationClusterService.EB_ADDRESS_PREFIX)) {
            return null;
        }
        return address.substring(NotificationClusterService.EB_ADDRESS_PREFIX.length());
    }

    /**
     * Parses the REGISTER payload's token using the configured verification and issuer
     * settings and returns its subject. Returns {@code null} if the token or subject is
     * missing, or token parsing fails. The caller compares the subject with the requested receiver.
     */
    String extractSubjectFromRegisterPayload(BridgeEvent bridgeEvent) {
        String token = bridgeEvent.getRawMessage().getString(REGISTER_TOKEN_FIELD);
        if (token == null || token.isBlank()) {
            return null;
        }

        try {
            var parsedToken = tokenParserService
                    .parseToken(new TokenParserRequest(token).verify(notificationConfig.tokenConfig().verified())
                            .issuerEnabled(notificationConfig.tokenConfig().publicKeyEnabled())
                            .issuerSuffix(notificationConfig.tokenConfig().publicKeyLocationSuffix()));

            return parsedToken.getClaim("sub");
        } catch (Exception ex) {
            LOG.debugf("Failed to read token subject from register payload: %s", ex.getMessage());
            return null;
        }
    }

    /** Raw (non-Mutiny) Vert.x instance — needed to create the {@link SockJSHandler}. */
    @Inject
    Vertx vertx;

    /** The Quarkus-managed HTTP router on which the SockJS sub-router is mounted. */
    @Inject
    Router router;

    /** Cluster service used to drain the IMap inbox after a matching REGISTERED confirmation. */
    @Inject
    NotificationClusterService cacheManager;

    /** Used to serialize {@link NotificationDTO} instances to JSON before publishing to the local EventBus. */
    @Inject
    ObjectMapper objectMapper;

    /** REST client for calling the downstream SVC to mark notifications as delivered. */
    @Inject
    @RestClient
    NotificationInternalApi notificationSVCClient;

    /** Token parser service for extracting claims from JWT tokens. */
    @Inject
    TokenParserService tokenParserService;

    private static final String REGISTER_TOKEN_FIELD = "token";

    private static final String ADDRESS_FIELD = "address";

    /**
     * Configures and mounts the SockJS EventBus bridge at application startup.
     *
     * Creates a {@link SockJSHandler} with a 25-second heartbeat and a 30-second session
     * timeout, then wraps it with a {@link SockJSBridgeOptions} that permits only outbound
     * traffic on the {@code notifications.onecx.new.*} address namespace.
     *
     * A bridge event handler intercepts all SockJS lifecycle events:
     * - SOCKET_CREATED — logs the new connection's remote address.
     * - REGISTER — validates the token subject against the {@code receiverId}
     * and tracks the pending request.
     * - REGISTERED — after the SockJS bridge accepts the registration:
     * 1. Activates the matching request, unless it has been cancelled or already activated.
     * 2. Calls {@link NotificationClusterService#consumeByReceiverId(String)} to
     * atomically drain the IMap inbox.
     * 3. Publishes each drained notification as JSON to the raw (non-Mutiny) local
     * EventBus so the bridge immediately forwards it to the waiting browser.
     * 4. For each drained notification with {@code persist=true}, calls
     * {@code markNotificationAsDelivered(id)} on the downstream SVC.
     * - UNREGISTER cancels only that address registration; SOCKET_CLOSED and SOCKET_ERROR
     * cancel all pending and active registrations for that socket.
     *
     * Accepted bridge events call {@code bridgeEvent.complete(true)}. A REGISTER event
     * with a mismatched token subject is rejected.
     */
    void onStart(@Observes StartupEvent event) {
        // sessionTimeout: how long a session survives without any client frame (ms)
        // heartbeatInterval: server sends a heartbeat frame every N ms to keep connection alive
        SockJSHandlerOptions sockJSOptions = new SockJSHandlerOptions()
                .setHeartbeatInterval(Integer.parseInt(notificationConfig.websocketConfig().heartbeatInterval()))
                .setSessionTimeout(Integer.parseInt(notificationConfig.websocketConfig().sessionTimeout()))
                // The per-socket registry uses writeHandlerID() as its key.
                .setRegisterWriteHandler(true);
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx, sockJSOptions);

        // Escape dots for regex: "notifications.new." → "notifications\.new\."
        String addressRegex = NotificationClusterService.EB_ADDRESS_PREFIX
                .replace(".", "\\.") + ".+";

        SockJSBridgeOptions bridgeOptions = new SockJSBridgeOptions()
                // Server → browser: only the notifications.new.* namespace
                .addOutboundPermitted(new PermittedOptions().setAddressRegex(addressRegex))
                // ping timeout: how long the bridge waits for a pong before closing (ms)
                .setPingTimeout(Integer.parseInt(notificationConfig.websocketConfig().pingTimeout()));
        // No inbound permitted — browsers cannot publish to the EventBus

        router.route(BRIDGE_PATH).subRouter(
                sockJSHandler.bridge(bridgeOptions, this::handleBridgeEvent));

        LOG.info("SockJS EventBus bridge mounted on " + BRIDGE_PATH);
    }

    /**
     * Coordinates authorization and registration state changes for SockJS lifecycle events.
     * REGISTER tracks an authorized request; REGISTERED activates it and starts inbox replay
     * only if activation is new. UNREGISTER removes one subscription, while socket close/error
     * removes all subscriptions on that connection.
     * Rejects a notification registration when its receiver does not match the token subject;
     * other events complete successfully here and remain subject to the bridge permissions.
     */
    void handleBridgeEvent(BridgeEvent bridgeEvent) {
        switch (bridgeEvent.type()) {
            case SOCKET_CREATED -> LOG.infof("SockJS socket created: %s", bridgeEvent.socket().remoteAddress());
            case SOCKET_CLOSED -> {
                LOG.infof("SockJS socket closed:   %s", bridgeEvent.socket().remoteAddress());
                removeSocket(bridgeEvent.socket().writeHandlerID());
            }
            case SOCKET_ERROR -> removeSocket(bridgeEvent.socket().writeHandlerID());
            case REGISTER -> {
                String address = bridgeEvent.getRawMessage().getString(ADDRESS_FIELD);
                String receiverId = receiverIdFromAddress(address);
                if (receiverId != null) {
                    String subject = extractSubjectFromRegisterPayload(bridgeEvent);
                    if (!receiverId.equals(subject)) {
                        LOG.warnf(
                                "Rejecting SockJS register for address='%s': token subject '%s' is not allowed to subscribe as receiverId '%s'",
                                address, subject, receiverId);
                        bridgeEvent.complete(false);
                        return;
                    }
                    trackRegistration(bridgeEvent.socket().writeHandlerID(), receiverId, bridgeEvent.getRawMessage());
                } else {
                    LOG.debugf("Client registered on: %s", address);
                }
            }
            case REGISTERED -> {
                String address = bridgeEvent.getRawMessage().getString(ADDRESS_FIELD);
                String receiverId = receiverIdFromAddress(address);
                if (receiverId != null && activateRegistration(
                        bridgeEvent.socket().writeHandlerID(), receiverId, bridgeEvent.getRawMessage())) {
                    LOG.infof("Client registered for receiverId='%s' — draining inbox", receiverId);
                    drainInbox(receiverId, address, vertx.eventBus());
                } else {
                    LOG.debugf("Ignoring inactive or duplicate SockJS registration for address='%s'", address);
                }
            }
            case UNREGISTER -> {
                String receiverId = receiverIdFromAddress(bridgeEvent.getRawMessage().getString(ADDRESS_FIELD));
                if (receiverId != null) {
                    removeRegistration(bridgeEvent.socket().writeHandlerID(), receiverId);
                }
            }
            default -> {
                // Other bridge events do not change notification registrations.
            }
        }

        bridgeEvent.complete(true);
    }

    /**
     * Starts asynchronous consumption of the receiver's stored inbox after a new activation.
     * Publishes the stored notifications to the receiver's local event bus address, so all
     * sockets subscribed to that address can receive them, not just the newly registered socket.
     * Persisted notifications are marked delivered through a REST call on a worker thread.
     * This method does not change registration state.
     */
    private void drainInbox(String receiverId, String address, EventBus rawEventBus) {
        cacheManager.consumeByReceiverId(receiverId)
                .subscribe().with(
                        notifications -> {
                            if (notifications.isEmpty()) {
                                LOG.debugf("No stored notifications for receiverId='%s'", receiverId);
                                return;
                            }
                            LOG.infof("Replaying %d stored notification(s) to '%s'", notifications.size(), address);
                            for (NotificationDTO n : notifications) {
                                try {
                                    String json = objectMapper.writeValueAsString(n);
                                    rawEventBus.publish(address, json);
                                    if (Boolean.TRUE.equals(n.getPersist()) && n.getId() != null) {
                                        final String notifId = n.getId();
                                        vertx.executeBlocking(() -> {
                                            @SuppressWarnings("resource")
                                            var r = notificationSVCClient.markNotificationAsDelivered(notifId);
                                            return r.getStatus();
                                        }).onSuccess(status -> LOG.debugf(
                                                "Marked stored notification id='%s' as delivered (status=%d)",
                                                notifId, status))
                                                .onFailure(ex -> LOG.warnf(
                                                        "Failed to mark stored notification id='%s' as delivered: %s",
                                                        notifId, ex.getMessage()));
                                    }
                                } catch (Exception e) {
                                    LOG.errorf("Failed to serialize notification: %s", e.getMessage());
                                }
                            }
                        },
                        err -> LOG.errorf("Failed to drain inbox for receiverId='%s': %s",
                                receiverId, err.getMessage()));
    }
}
