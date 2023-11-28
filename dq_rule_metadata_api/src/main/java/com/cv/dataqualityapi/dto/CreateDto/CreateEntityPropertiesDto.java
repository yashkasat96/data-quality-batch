package com.cv.dataqualityapi.dto.CreateDto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateEntityPropertiesDto {

    private Integer entitypropId;

    private Integer EntityId;

    private String entitypropKey;

    private String entitypropValue;

    private String entitypropCreatedBy;

    private String entitypropUpdatedBy;

    private String entitypropCreatedDate;

    private String entitypropUpdatedDate;
}