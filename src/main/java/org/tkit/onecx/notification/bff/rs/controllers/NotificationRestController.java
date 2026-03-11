package org.tkit.onecx.notification.bff.rs.controllers;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.core.Response;

import org.tkit.onecx.notification.bff.rs.service.NotificationClusterService;
import org.tkit.quarkus.log.cdi.LogService;

import gen.org.tkit.onecx.notification.bff.rs.internal.NotificationInternalApiService;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;

@ApplicationScoped
@Transactional(value = Transactional.TxType.NOT_SUPPORTED)
@LogService
public class NotificationRestController implements NotificationInternalApiService {

    @Inject
    NotificationClusterService cacheManager;

    @Override
    public Response dispatchNotification(NotificationDTO notificationDTO) {
        cacheManager.storeNotification(notificationDTO).await().indefinitely();
        return Response.ok().build();
    }
}
