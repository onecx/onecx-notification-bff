package org.tkit.onecx.notification.bff.rs.ws;

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
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.BridgeEvent;
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;

/**
 * Mounts the Vert.x SockJS EventBus bridge and handles the full browser-facing
 * WebSocket lifecycle for real-time notification delivery.
 *
 * How the bridge works:
 * The Vert.x SockJS EventBus bridge exposes the internal Vert.x EventBus to browsers
 * over a SockJS connection (HTTP long-poll / WebSocket upgrade). Browsers connect to
 * {@code /eventbus/*} and communicate using the SockJS EventBus protocol (JSON frames).
 *
 * Connection lifecycle:
 * 1. SOCKET_CREATED — a browser has opened the SockJS connection. Logged only.
 * 2. REGISTER — the browser subscribes to an EventBus address of the form
 * {@code notifications.onecx.new.<receiverId>}. At this point:
 * - The JWT token must be present in the JSON payload as the {@code "token"} field.
 * - The token subject ({@code sub}) is extracted and compared to the {@code receiverId}.
 * - If they do not match, the registration is rejected.
 * - If they match, the socket-to-receiver association is recorded so the cluster service
 * knows this pod has a live session for that receiver.
 * - The IMap inbox is drained via
 * {@link NotificationClusterService#consumeByReceiverId(String)} and each
 * stored notification is immediately published to the local EventBus so the
 * bridge delivers it to the client — no separate HTTP call needed.
 * - Drained notifications with {@code persist=true} are marked as delivered
 * via the downstream SVC REST client.
 * 3. SOCKET_CLOSED — the SockJS connection was closed. Only that socket's association
 * is removed. A receiver remains active while any registered socket for it is open.
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
     * Pod-local mapping of registered SockJS socket IDs to receiverIds.
     *
     * Updated on authorized REGISTER and SOCKET_CLOSED. Stored in a
     * {@link ConcurrentHashMap} because the Hazelcast topic listener in
     * {@link NotificationClusterService} reads it from a non-Vert.x thread.
     *
     * The registry is deliberately pod-local: each pod independently tracks its own
     * connected sockets, which is all that is needed to decide whether to clear the
     * IMap entry after live delivery on that pod.
     */
    private static final ConcurrentHashMap<String, String> ACTIVE_SOCKETS = new ConcurrentHashMap<>();

    /**
     * Returns {@code true} if the given {@code receiverId} has at least one open SockJS
     * session on this pod.
     *
     * Called by {@link NotificationClusterService}'s topic listener to decide whether
     * to remove the IMap entry after publishing to the local EventBus (live delivery path).
     * If no active session exists, the IMap entry is left intact so the notification can
     * be drained the next time the client connects and sends a REGISTER frame.
     *
     * @param receiverId the receiver to check
     * @return {@code true} if the receiver is currently connected on this pod
     */
    public static boolean hasActiveReceiver(String receiverId) {
        return receiverId != null && ACTIVE_SOCKETS.containsValue(receiverId);
    }

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

    /** The Quarkus-managed Vert.x HTTP router on which the SockJS sub-router is mounted. */
    @Inject
    Router router;

    /** Cluster service used to drain the IMap inbox on REGISTER. */
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

    /**
     * Configures and mounts the SockJS EventBus bridge at application startup.
     *
     * Creates a {@link SockJSHandler} with a 25-second heartbeat and a 30-second session
     * timeout, then wraps it with a {@link SockJSBridgeOptions} that permits only outbound
     * traffic on the {@code notifications.onecx.new.*} address namespace.
     *
     * A bridge event handler intercepts all SockJS lifecycle events:
     * - SOCKET_CREATED — logs the new connection's remote address.
     * - SOCKET_CLOSED — removes the closing socket's association. A receiver remains
     * active while another registered socket for it remains open.
     * - REGISTER — when the address matches the notification namespace:
     * 1. Extracts the {@code receiverId} from the address suffix.
     * 2. Records the socket-to-receiver association after validating the token subject.
     * 3. Calls {@link NotificationClusterService#consumeByReceiverId(String)} to
     * atomically drain the IMap inbox.
     * 4. Publishes each drained notification as JSON to the raw (non-Mutiny) local
     * EventBus so the bridge immediately forwards it to the waiting browser.
     * 5. For each drained notification with {@code persist=true}, calls
     * {@code markNotificationAsDelivered(id)} on the downstream SVC.
     *
     * All bridge events call {@code bridgeEvent.complete(true)} to allow the event to
     * proceed normally (i.e. nothing is blocked or rejected here).
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

        // Get raw (non-Mutiny) EventBus for local publish inside the bridge handler
        EventBus rawEventBus = vertx.eventBus();

        router.route(BRIDGE_PATH).subRouter(
                sockJSHandler.bridge(bridgeOptions, bridgeEvent -> {
                    if (bridgeEvent.type() == BridgeEventType.SOCKET_CREATED) {
                        LOG.infof("SockJS socket created: %s", bridgeEvent.socket().remoteAddress());

                    } else if (bridgeEvent.type() == BridgeEventType.SOCKET_CLOSED) {
                        LOG.infof("SockJS socket closed:   %s", bridgeEvent.socket().remoteAddress());
                        ACTIVE_SOCKETS.remove(bridgeEvent.socket().writeHandlerID());

                    } else if (bridgeEvent.type() == BridgeEventType.REGISTER) {
                        String address = bridgeEvent.getRawMessage().getString("address");

                        if (address.startsWith(NotificationClusterService.EB_ADDRESS_PREFIX)) {
                            // Extract receiverId from "notifications.new.<receiverId>"
                            String receiverId = address.substring(
                                    NotificationClusterService.EB_ADDRESS_PREFIX.length());
                            String subject = extractSubjectFromRegisterPayload(bridgeEvent);

                            if (!receiverId.equals(subject)) {
                                LOG.warnf(
                                        "Rejecting SockJS register for address='%s': token subject '%s' is not allowed to subscribe as receiverId '%s'",
                                        address, subject, receiverId);
                                bridgeEvent.complete(false);
                                return;
                            }

                            // Track this socket once; repeated registrations overwrite the same entry.
                            ACTIVE_SOCKETS.put(bridgeEvent.socket().writeHandlerID(), receiverId);
                            LOG.infof("Client registered for receiverId='%s' (sub='%s') — draining inbox",
                                    receiverId, subject);

                            // Drain IMap inbox: read + remove, then push each stored notification
                            cacheManager.consumeByReceiverId(receiverId)
                                    .subscribe().with(
                                            notifications -> {
                                                if (notifications.isEmpty()) {
                                                    LOG.debugf("No stored notifications for receiverId='%s'",
                                                            receiverId);
                                                    return;
                                                }
                                                LOG.infof("Replaying %d stored notification(s) to '%s'",
                                                        notifications.size(), address);
                                                for (NotificationDTO n : notifications) {
                                                    try {
                                                        String json = objectMapper.writeValueAsString(n);
                                                        // publish locally — SockJS bridge delivers to this client
                                                        rawEventBus.publish(address, json);
                                                        // Mark persisted notifications as delivered.
                                                        // Must run on a worker thread — the RESTEasy Reactive
                                                        // blocking client cannot be called from the event loop.
                                                        if (Boolean.TRUE.equals(n.getPersist()) && n.getId() != null) {
                                                            final String notifId = n.getId();
                                                            vertx.executeBlocking(() -> {
                                                                @SuppressWarnings("resource")
                                                                var r = notificationSVCClient
                                                                        .markNotificationAsDelivered(notifId);
                                                                return r.getStatus();
                                                            }).onSuccess(status -> LOG.debugf(
                                                                    "Marked stored notification id='%s' as delivered (status=%d)",
                                                                    notifId, status))
                                                                    .onFailure(ex -> LOG.warnf(
                                                                            "Failed to mark stored notification id='%s' as delivered: %s",
                                                                            notifId, ex.getMessage()));
                                                        }
                                                    } catch (Exception e) {
                                                        LOG.errorf("Failed to serialize notification: %s",
                                                                e.getMessage());
                                                    }
                                                }
                                            },
                                            err -> LOG.errorf(
                                                    "Failed to drain inbox for receiverId='%s': %s",
                                                    receiverId, err.getMessage()));
                        } else {
                            LOG.debugf("Client registered on: %s", address);
                        }
                    }

                    bridgeEvent.complete(true);
                }));

        LOG.info("SockJS EventBus bridge mounted on " + BRIDGE_PATH);
    }
}
