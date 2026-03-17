package org.tkit.onecx.notification.bff.rs.controllers;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tkit.onecx.notification.bff.rs.AbstractTest;
import org.tkit.onecx.notification.bff.rs.service.NotificationClusterService;
import org.tkit.onecx.notification.bff.rs.ws.NotificationSockJSBridge;
import org.tkit.quarkus.security.test.GenerateKeycloakClient;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.ContentMetaDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.IssuerDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.SeverityDTO;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@GenerateKeycloakClient(clientName = "testClient", scopes = { "ocx-no:all", "ocx-no:read", "ocx-no:write", "ocx-no:delete" })
class NotificationRestControllerTest extends AbstractTest {

    @Inject
    NotificationClusterService clusterService;

    @BeforeEach
    void drainAll() {
        // Clean up stored notifications between tests so tests are independent.
        // We drain a fixed set of receiverIds used in the tests.
        for (String r : List.of("receiver1", "receiver2", "noReceiver")) {
            clusterService.consumeByReceiverId(r).await().indefinitely();
        }
    }

    // -------------------------------------------------------------------------
    // POST /notifications/dispatch
    // -------------------------------------------------------------------------

    @Test
    void dispatch_happyPath_returns200() {
        NotificationDTO dto = buildNotification("receiver1", false);

        given()
                .auth().oauth2(getKeycloakClientToken("testClient"))
                .contentType("application/json")
                .body(dto)
                .when().post("/notifications/dispatch")
                .then()
                .statusCode(200);
    }

    @Test
    void dispatch_withPersistTrue_returns200() {
        NotificationDTO dto = buildNotification("receiver1", true);
        dto.setId("notif-persist-id");

        given()
                .auth().oauth2(getKeycloakClientToken("testClient"))
                .contentType("application/json")
                .body(dto)
                .when().post("/notifications/dispatch")
                .then()
                .statusCode(200);
    }

    @Test
    void dispatch_withContentMeta_returns200() {
        NotificationDTO dto = buildNotification("receiver2", false);
        ContentMetaDTO meta = new ContentMetaDTO();
        meta.setKey("foo");
        meta.setValue("bar");
        dto.setContentMeta(List.of(meta));

        given()
                .auth().oauth2(getKeycloakClientToken("testClient"))
                .contentType("application/json")
                .body(dto)
                .when().post("/notifications/dispatch")
                .then()
                .statusCode(200);
    }

    @Test
    void dispatch_withSeverityAndIssuer_returns200() {
        NotificationDTO dto = buildNotification("receiver1", false);
        dto.setSeverity(SeverityDTO.CRITICAL);
        dto.setIssuer(IssuerDTO.SYSTEM);

        given()
                .auth().oauth2(getKeycloakClientToken("testClient"))
                .contentType("application/json")
                .body(dto)
                .when().post("/notifications/dispatch")
                .then()
                .statusCode(200);
    }

    @Test
    void dispatch_withoutContentMeta_returns200() {
        // contentMeta is optional — omitting it must not cause a 400
        NotificationDTO dto = buildNotification("receiver1", false);
        dto.setContentMeta(null);

        given()
                .auth().oauth2(getKeycloakClientToken("testClient"))
                .contentType("application/json")
                .body(dto)
                .when().post("/notifications/dispatch")
                .then()
                .statusCode(200);
    }

    // -------------------------------------------------------------------------
    // Integration with NotificationClusterService — verify the notification
    // is actually stored and can be retrieved via consumeByReceiverId()
    // -------------------------------------------------------------------------

    @Test
    void dispatch_storesNotificationInCluster() {
        NotificationDTO dto = buildNotification("receiver1", false);
        dto.setApplicationId("app-test");
        dto.setSenderId("sender-test");

        given()
                .auth().oauth2(getKeycloakClientToken("testClient"))
                .contentType("application/json")
                .body(dto)
                .when().post("/notifications/dispatch")
                .then()
                .statusCode(200);

        List<NotificationDTO> stored = clusterService.consumeByReceiverId("receiver1")
                .await().indefinitely();

        assertThat(stored).hasSize(1);
        assertThat(stored.get(0).getReceiverId()).isEqualTo("receiver1");
        assertThat(stored.get(0).getApplicationId()).isEqualTo("app-test");
        assertThat(stored.get(0).getSenderId()).isEqualTo("sender-test");
        assertThat(stored.get(0).getPersist()).isFalse();
    }

    @Test
    void dispatch_multipleToSameReceiver_allStored() {
        for (int i = 0; i < 3; i++) {
            NotificationDTO dto = buildNotification("receiver2", false);
            dto.setSenderId("sender-" + i);
            given()
                    .auth().oauth2(getKeycloakClientToken("testClient"))
                    .contentType("application/json")
                    .body(dto)
                    .when().post("/notifications/dispatch")
                    .then()
                    .statusCode(200);
        }

        List<NotificationDTO> stored = clusterService.consumeByReceiverId("receiver2")
                .await().indefinitely();
        assertThat(stored).hasSize(3);
    }

    @Test
    void dispatch_toDifferentReceivers_storedSeparately() {
        given()
                .auth().oauth2(getKeycloakClientToken("testClient"))
                .contentType("application/json")
                .body(buildNotification("receiver1", false))
                .when().post("/notifications/dispatch")
                .then().statusCode(200);

        given()
                .auth().oauth2(getKeycloakClientToken("testClient"))
                .contentType("application/json")
                .body(buildNotification("receiver2", false))
                .when().post("/notifications/dispatch")
                .then().statusCode(200);

        assertThat(clusterService.consumeByReceiverId("receiver1").await().indefinitely()).hasSize(1);
        assertThat(clusterService.consumeByReceiverId("receiver2").await().indefinitely()).hasSize(1);
    }

    // -------------------------------------------------------------------------
    // NotificationClusterService — consumeByReceiverId edge cases
    // -------------------------------------------------------------------------

    @Test
    void consumeByReceiverId_unknownReceiver_returnsEmptyList() {
        List<NotificationDTO> result = clusterService.consumeByReceiverId("nobody")
                .await().indefinitely();
        assertThat(result).isEmpty();
    }

    @Test
    void consumeByReceiverId_nullReceiverId_returnsEmptyList() {
        List<NotificationDTO> result = clusterService.consumeByReceiverId(null)
                .await().indefinitely();
        assertThat(result).isEmpty();
    }

    @Test
    void consumeByReceiverId_blankReceiverId_returnsEmptyList() {
        List<NotificationDTO> result = clusterService.consumeByReceiverId("   ")
                .await().indefinitely();
        assertThat(result).isEmpty();
    }

    @Test
    void consumeByReceiverId_removesEntryAfterConsume() {
        given()
                .auth().oauth2(getKeycloakClientToken("testClient"))
                .contentType("application/json")
                .body(buildNotification("receiver1", false))
                .when().post("/notifications/dispatch")
                .then().statusCode(200);

        // First consume drains the inbox
        List<NotificationDTO> first = clusterService.consumeByReceiverId("receiver1")
                .await().indefinitely();
        assertThat(first).hasSize(1);

        // Second consume must return empty — not a duplicate delivery
        List<NotificationDTO> second = clusterService.consumeByReceiverId("receiver1")
                .await().indefinitely();
        assertThat(second).isEmpty();
    }

    // -------------------------------------------------------------------------
    // NotificationSockJSBridge — hasActiveReceiver (static pod-local registry)
    // -------------------------------------------------------------------------

    @Test
    void hasActiveReceiver_unknownReceiver_returnsFalse() {
        assertThat(NotificationSockJSBridge.hasActiveReceiver("not-registered")).isFalse();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private NotificationDTO buildNotification(String receiverId, boolean persist) {
        NotificationDTO dto = new NotificationDTO();
        dto.setApplicationId("app1");
        dto.setSenderId("sender");
        dto.setReceiverId(receiverId);
        dto.setPersist(persist);
        return dto;
    }
}
