package com.cv.dataqualityapi.dto.CreateDto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateEntitiesDto {

    private Integer entityId;

    private Integer entitytemplateId;

    private String entityPhysicalName;

    private String entityName;

    private String entityPrimaryKey;

    private String createdBy;

    private String updatedBy;

    private String createdDate;

    private String updatedDate;
}