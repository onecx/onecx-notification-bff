package org.tkit.onecx.notification.bff.rs.controllers;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.tkit.onecx.notification.bff.rs.AbstractTest;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.IssuerDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationRetrieveRequestDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationRetrieveResponseDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.SeverityDTO;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.keycloak.client.KeycloakTestClient;
import io.restassured.http.ContentType;

//@QuarkusTest
class NotificationRestControllerTest extends AbstractTest {
//
//    private final KeycloakTestClient keycloakClient = new KeycloakTestClient();
//
//    // ------------------------------------------------------------------
//    // dispatchNotification
//    // ------------------------------------------------------------------
//
//    @Test
//    void dispatchNotification_returns200() {
//        NotificationDTO notification = buildNotification("app1", "user1");
//
//        given()
//                .auth().oauth2(keycloakClient.getAccessToken(ADMIN))
//                .contentType(ContentType.JSON)
//                .body(notification)
//                .when()
//                .post("/notifications/dispatch")
//                .then()
//                .statusCode(200);
//    }
//
//    @Test
//    void dispatchNotification_missingBody_returns400() {
//        given()
//                .auth().oauth2(keycloakClient.getAccessToken(ADMIN))
//                .contentType(ContentType.JSON)
//                .when()
//                .post("/notifications/dispatch")
//                .then()
//                .statusCode(400);
//    }
//
//    // ------------------------------------------------------------------
//    // retrieveNotifications
//    // ------------------------------------------------------------------
//
//    @Test
//    void retrieveNotifications_returnsStoredNotification() {
//        String receiverId = "retrieve-test-user-" + System.nanoTime();
//        String appId = "app-retrieve";
//
//        // dispatch first
//        NotificationDTO notification = buildNotification(appId, receiverId);
//        given()
//                .auth().oauth2(keycloakClient.getAccessToken(ADMIN))
//                .contentType(ContentType.JSON)
//                .body(notification)
//                .when()
//                .post("/notifications/dispatch")
//                .then()
//                .statusCode(200);
//
//        // now retrieve
//        NotificationRetrieveRequestDTO request = new NotificationRetrieveRequestDTO()
//                .receiverId(receiverId);
//
//        NotificationRetrieveResponseDTO response = given()
//                .auth().oauth2(keycloakClient.getAccessToken(ADMIN))
//                .contentType(ContentType.JSON)
//                .body(request)
//                .when()
//                .post("/notifications/retrieve")
//                .then()
//                .statusCode(200)
//                .extract().as(NotificationRetrieveResponseDTO.class);
//
//        List<NotificationDTO> notifications = response.getNotifications();
//        assertThat(notifications).isNotEmpty();
//        assertThat(notifications.get(0).getReceiverId()).isEqualTo(receiverId);
//        assertThat(notifications.get(0).getApplicationId()).isEqualTo(appId);
//    }
//
//    @Test
//    void retrieveNotifications_unknownReceiver_returnsEmptyList() {
//        NotificationRetrieveRequestDTO request = new NotificationRetrieveRequestDTO()
//                .receiverId("no-such-user-" + System.nanoTime());
//
//        NotificationRetrieveResponseDTO response = given()
//                .auth().oauth2(keycloakClient.getAccessToken(USER))
//                .contentType(ContentType.JSON)
//                .body(request)
//                .when()
//                .post("/notifications/retrieve")
//                .then()
//                .statusCode(200)
//                .extract().as(NotificationRetrieveResponseDTO.class);
//
//        assertThat(response.getNotifications()).isEmpty();
//    }
//
//    // ------------------------------------------------------------------
//    // Security
//    // ------------------------------------------------------------------
//
//    @Test
//    void dispatch_unauthenticated_returns401() {
//        given()
//                .contentType(ContentType.JSON)
//                .body(buildNotification("app", "user"))
//                .when()
//                .post("/notifications/dispatch")
//                .then()
//                .statusCode(401);
//    }
//
//    // ------------------------------------------------------------------
//    // Helpers
//    // ------------------------------------------------------------------
//
//    private NotificationDTO buildNotification(String appId, String receiverId) {
//        return new NotificationDTO()
//                .applicationId(appId)
//                .senderId("sender-1")
//                .receiverId(receiverId)
//                .persist(false)
//                .severity(SeverityDTO.NORMAL)
//                .issuer(IssuerDTO.SYSTEM);
//    }
}
