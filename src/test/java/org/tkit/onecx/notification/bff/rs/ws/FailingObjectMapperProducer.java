package org.tkit.onecx.notification.bff.rs.ws;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * CDI alternative producer that replaces the standard {@link ObjectMapper} with one
 * that throws {@link JsonProcessingException} on every {@code writeValueAsString} call.
 *
 * Activated only when {@link NotificationSockJSBridgeSerializationErrorTest} runs via
 * its {@link io.quarkus.test.junit.QuarkusTestProfile} using {@code getEnabledAlternatives()}.
 * Without that explicit activation this class has no effect.
 */
@Alternative
@ApplicationScoped
public class FailingObjectMapperProducer {

    @Produces
    @ApplicationScoped
    public ObjectMapper failingObjectMapper() {
        return new ObjectMapper() {
            @Override
            public String writeValueAsString(Object value) throws JsonProcessingException {
                throw new JsonProcessingException("simulated serialization failure") {
                };
            }
        };
    }
}
