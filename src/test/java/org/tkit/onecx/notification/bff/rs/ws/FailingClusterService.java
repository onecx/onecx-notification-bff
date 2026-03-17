package org.tkit.onecx.notification.bff.rs.ws;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;

import org.tkit.onecx.notification.bff.rs.service.NotificationClusterService;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import io.smallrye.mutiny.Uni;

/**
 * CDI alternative for {@link NotificationClusterService} that makes
 * {@link #consumeByReceiverId(String)} return a failing {@link Uni}.
 * Activated only via DrainErrorProfile — has no effect in normal test runs.
 */
@Alternative
@ApplicationScoped
public class FailingClusterService extends NotificationClusterService {

    @Override
    public Uni<List<NotificationDTO>> consumeByReceiverId(String receiverId) {
        return Uni.createFrom().failure(new RuntimeException("simulated drain failure"));
    }
}
