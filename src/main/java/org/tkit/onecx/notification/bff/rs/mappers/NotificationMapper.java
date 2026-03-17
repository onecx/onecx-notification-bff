package org.tkit.onecx.notification.bff.rs.mappers;

import java.util.List;

import org.mapstruct.Mapper;
import org.tkit.quarkus.rs.mappers.OffsetDateTimeMapper;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationRetrieveResponseDTO;

@Mapper(uses = OffsetDateTimeMapper.class)
public interface NotificationMapper {

    /**
     * Wrap a list of notifications into the retrieve-response envelope DTO.
     */
    default NotificationRetrieveResponseDTO toResponse(List<NotificationDTO> notifications) {
        NotificationRetrieveResponseDTO response = new NotificationRetrieveResponseDTO();
        response.setNotifications(notifications != null ? notifications : List.of());
        return response;
    }
}
