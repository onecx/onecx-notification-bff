package org.tkit.onecx.notification.bff.rs.ws;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * CDI alternative that replaces the standard {@link ObjectMapper} with one that
 * throws {@link JsonProcessingException} on every {@code readValue} call.
 *
 * Activated only via {@code NotificationClusterServiceDeserializationErrorTest}
 * using {@code getEnabledAlternatives()} — has no effect in normal test runs.
 */
@Alternative
@ApplicationScoped
public class FailingReadObjectMapperProducer {

    @Produces
    @ApplicationScoped
    public ObjectMapper failingReadObjectMapper() {
        return new ObjectMapper() {
            @Override
            public <T> T readValue(String content, Class<T> valueType) throws JsonProcessingException {
                throw new JsonProcessingException("simulated deserialization failure") {
                };
            }
        };
    }
}
