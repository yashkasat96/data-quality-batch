package com.cv.dataqualityapi.dto.CreateDto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateEntityTemplatePropertiesDto {

    private Integer entitytemplatepropId;

    private Integer entitytemplateId;

    private String entitytemplatepropKey;

    private String entitytemplatepropDesc;

    private String entitytemplatepropType;

    private String isMandatory;

    private String entitytemplatepropCreatedBy;

    private String entitytemplatepropCreatedDate;

    private String entitytemplatepropUpdatedBy;

    private String entitytemplatepropUpdatedDate;
}