package org.tkit.onecx.notification.bff.rs.mappers;

import java.util.List;

import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationRetrieveResponseDTO;

/**
 * MapStruct mapper for notification DTO conversions.
 *
 * The domain model is the generated NotificationDTO (already Serializable and
 * stored directly in the Hazelcast IMap), so this mapper's primary job is
 * wrapping a list of notifications into the retrieve-response envelope.
 */
@Mapper(componentModel = MappingConstants.ComponentModel.CDI)
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
