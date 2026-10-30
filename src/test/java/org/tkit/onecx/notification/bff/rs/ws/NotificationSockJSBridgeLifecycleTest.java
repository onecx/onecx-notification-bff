package org.tkit.onecx.notification.bff.rs.ws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.tkit.onecx.notification.bff.rs.service.NotificationClusterService;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.web.handler.sockjs.BridgeEvent;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;

@QuarkusTest
class NotificationSockJSBridgeLifecycleTest {

    private NotificationSockJSBridge bridge;
    private final List<SockJSSocket> sockets = new ArrayList<>();
    private final String receiverId = UUID.randomUUID().toString();

    @BeforeEach
    void setUp() {
        bridge = spy(new NotificationSockJSBridge());
        bridge.vertx = mock(Vertx.class);
        bridge.cacheManager = mock(NotificationClusterService.class);
        when(bridge.cacheManager.consumeByReceiverId(anyString())).thenReturn(Uni.createFrom().item(List.of()));
        doAnswer(invocation -> invocation.<BridgeEvent> getArgument(0).getRawMessage().getString("token"))
                .when(bridge).extractSubjectFromRegisterPayload(any());
    }

    @AfterEach
    void closeSockets() {
        for (SockJSSocket socket : sockets) {
            handle(BridgeEventType.SOCKET_CLOSED, socket, null);
        }
    }

    @ParameterizedTest
    @EnumSource(value = BridgeEventType.class, names = { "UNREGISTER", "SOCKET_CLOSED", "SOCKET_ERROR" })
    void cancelledRegistrationCannotBecomeActiveOrDrainInbox(BridgeEventType cancellation) {
        SockJSSocket socket = socket();
        JsonObject registration = registration(receiverId);
        handle(BridgeEventType.REGISTER, socket, registration);
        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();

        handle(cancellation, socket, registration);
        handle(cancellation, socket, registration);
        handle(BridgeEventType.REGISTERED, socket, registration);

        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
        verifyNoInteractions(bridge.cacheManager);
    }

    @Test
    void oldCompletionCannotPromoteANewRegistrationForTheSameAddress() {
        SockJSSocket socket = socket();
        JsonObject oldRegistration = registration(receiverId);
        JsonObject newRegistration = registration(receiverId);
        handle(BridgeEventType.REGISTER, socket, oldRegistration);
        handle(BridgeEventType.UNREGISTER, socket, oldRegistration);
        handle(BridgeEventType.REGISTER, socket, newRegistration);

        handle(BridgeEventType.REGISTERED, socket, oldRegistration);
        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
        verifyNoInteractions(bridge.cacheManager);

        handle(BridgeEventType.REGISTERED, socket, newRegistration);
        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue();
        verify(bridge.cacheManager).consumeByReceiverId(receiverId);
    }

    @Test
    void duplicateRegisterDoesNotReplacePendingRegistrationOrDrainTwice() {
        SockJSSocket socket = socket();
        JsonObject registration = registration(receiverId);
        handle(BridgeEventType.REGISTER, socket, registration);
        handle(BridgeEventType.REGISTER, socket, registration(receiverId));
        handle(BridgeEventType.REGISTERED, socket, registration);
        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue();

        handle(BridgeEventType.REGISTER, socket, registration(receiverId));
        handle(BridgeEventType.REGISTERED, socket, registration);

        verify(bridge.cacheManager, times(1)).consumeByReceiverId(receiverId);
        handle(BridgeEventType.UNREGISTER, socket, registration);
        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = BridgeEventType.class, names = { "UNREGISTER", "SOCKET_CLOSED", "SOCKET_ERROR" })
    void activeConnectionCountTracksCompletedRegistrationsPerSocket(BridgeEventType cancellation) {
        SockJSSocket first = socket();
        SockJSSocket second = socket();
        JsonObject registration = registration(receiverId);
        assertThat(NotificationSockJSBridge.activeConnectionCount(receiverId)).isZero();

        register(first, receiverId);
        assertThat(NotificationSockJSBridge.activeConnectionCount(receiverId)).isEqualTo(1);

        handle(BridgeEventType.REGISTER, second, registration);
        assertThat(NotificationSockJSBridge.activeConnectionCount(receiverId)).isEqualTo(1);

        handle(BridgeEventType.REGISTERED, second, registration);
        assertThat(NotificationSockJSBridge.activeConnectionCount(receiverId)).isEqualTo(2);

        handle(BridgeEventType.REGISTERED, second, registration);
        assertThat(NotificationSockJSBridge.activeConnectionCount(receiverId)).isEqualTo(2);

        handle(cancellation, second, registration);
        assertThat(NotificationSockJSBridge.activeConnectionCount(receiverId)).isEqualTo(1);

        handle(cancellation, first, registration);
        assertThat(NotificationSockJSBridge.activeConnectionCount(receiverId)).isZero();
    }

    @ParameterizedTest
    @EnumSource(value = BridgeEventType.class, names = { "SOCKET_CLOSED", "SOCKET_ERROR" })
    void socketCleanupRemovesAllItsReceiversButPreservesOtherSockets(BridgeEventType cancellation) {
        SockJSSocket first = socket();
        SockJSSocket second = socket();
        String otherReceiver = UUID.randomUUID().toString();
        register(first, receiverId);
        register(first, otherReceiver);
        register(second, receiverId);

        handle(cancellation, first, null);
        handle(cancellation, first, null);
        assertThat(NotificationSockJSBridge.hasActiveReceiver(otherReceiver)).isFalse();
        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue();

        handle(cancellation, second, null);
        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
    }

    @Test
    void unregisterCancelsOnlyTheMatchingPendingRequest() {
        SockJSSocket socket = socket();
        String otherReceiver = UUID.randomUUID().toString();
        JsonObject cancelled = registration(receiverId);
        JsonObject pending = registration(otherReceiver);
        handle(BridgeEventType.REGISTER, socket, cancelled);
        handle(BridgeEventType.REGISTER, socket, pending);

        handle(BridgeEventType.UNREGISTER, socket, cancelled);
        handle(BridgeEventType.REGISTERED, socket, cancelled);
        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
        verifyNoInteractions(bridge.cacheManager);

        handle(BridgeEventType.REGISTERED, socket, pending);
        assertThat(NotificationSockJSBridge.hasActiveReceiver(otherReceiver)).isTrue();
        verify(bridge.cacheManager).consumeByReceiverId(otherReceiver);
    }

    @Test
    void unregisterPreservesOtherAddressesAndOtherSockets() {
        SockJSSocket first = socket();
        SockJSSocket second = socket();
        String otherReceiver = UUID.randomUUID().toString();
        register(first, receiverId);
        register(first, otherReceiver);
        register(second, receiverId);

        handle(BridgeEventType.UNREGISTER, first, registration(receiverId));
        handle(BridgeEventType.UNREGISTER, first, registration(receiverId));
        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue();
        assertThat(NotificationSockJSBridge.hasActiveReceiver(otherReceiver)).isTrue();

        handle(BridgeEventType.UNREGISTER, second, registration(receiverId));
        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
        assertThat(NotificationSockJSBridge.hasActiveReceiver(otherReceiver)).isTrue();
    }

    @Test
    void registrationWithoutSuccessfulCompletionDoesNotActivateOrDrain() {
        SockJSSocket socket = socket();
        handle(BridgeEventType.REGISTER, socket, registration(receiverId));

        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
        verifyNoInteractions(bridge.cacheManager);
    }

    @Test
    void rejectedRegistrationCannotBecomeActive() {
        SockJSSocket socket = socket();
        JsonObject registration = registration(receiverId).put("token", "different-subject");
        BridgeEvent event = event(BridgeEventType.REGISTER, socket, registration);

        bridge.handleBridgeEvent(event);
        verify(event).complete(false);
        handle(BridgeEventType.REGISTERED, socket, registration);

        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
        verifyNoInteractions(bridge.cacheManager);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "unrelated.address" })
    void ignoresRegistrationsOutsideNotificationNamespace(String address) {
        SockJSSocket socket = socket();
        JsonObject message = new JsonObject().put("address", address);
        handle(BridgeEventType.REGISTER, socket, message);
        handle(BridgeEventType.REGISTERED, socket, message);
        handle(BridgeEventType.UNREGISTER, socket, message);

        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isFalse();
        verifyNoInteractions(bridge.cacheManager);
    }

    @Test
    void otherEventsDoNotChangeRegistrations() {
        SockJSSocket socket = socket();
        register(socket, receiverId);

        handle(BridgeEventType.SOCKET_PING, socket, null);

        assertThat(NotificationSockJSBridge.hasActiveReceiver(receiverId)).isTrue();
        verify(bridge.cacheManager).consumeByReceiverId(receiverId);
    }

    private void register(SockJSSocket socket, String receiver) {
        JsonObject registration = registration(receiver);
        handle(BridgeEventType.REGISTER, socket, registration);
        handle(BridgeEventType.REGISTERED, socket, registration);
    }

    private SockJSSocket socket() {
        SockJSSocket socket = mock(SockJSSocket.class);
        when(socket.writeHandlerID()).thenReturn(UUID.randomUUID().toString());
        sockets.add(socket);
        handle(BridgeEventType.SOCKET_CREATED, socket, null);
        return socket;
    }

    private JsonObject registration(String receiver) {
        return new JsonObject()
                .put("address", NotificationClusterService.EB_ADDRESS_PREFIX + receiver)
                .put("token", receiver);
    }

    private void handle(BridgeEventType type, SockJSSocket socket, JsonObject message) {
        BridgeEvent event = event(type, socket, message);
        bridge.handleBridgeEvent(event);
        verify(event).complete(true);
    }

    private BridgeEvent event(BridgeEventType type, SockJSSocket socket, JsonObject message) {
        BridgeEvent event = mock(BridgeEvent.class);
        when(event.type()).thenReturn(type);
        when(event.socket()).thenReturn(socket);
        when(event.getRawMessage()).thenReturn(message);
        return event;
    }
}
