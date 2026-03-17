package org.tkit.onecx.notification.bff.rs.service;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import io.smallrye.mutiny.Uni;

/**
 * CDI alternative for {@link NotificationClusterService} that makes
 * {@link #removeFromMap(String)} return a failing {@link Uni}.
 *
 * This triggers the {@code err ->} error-handler branch in the
 * Hazelcast topic listener inside {@code onStart()} — the path reached when the
 * IMap {@code remove} operation itself fails after a notification has been
 * published to the topic with an active receiver present on this pod.
 *
 * Activated only via {@code NotificationClusterServiceRemoveErrorTest.RemoveErrorProfile}.
 */
@Alternative
@ApplicationScoped
public class FailingRemoveClusterService extends NotificationClusterService {

    @Override
    protected Uni<List<NotificationDTO>> removeFromMap(String receiverId) {
        return Uni.createFrom().failure(new RuntimeException("simulated IMap remove failure"));
    }
}
