package com.cv.dataqualityapi.dto.CreateDto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateRuleTemplatePropertiesDto {

    private Integer ruletemplatepropertiesId;

    private Integer ruletemplateId;

    private String ruletemplatepropertiesKey;

    private String ruletemplatepropertiesType;

    private String ruletemplatepropertiesValue;

    private String ruletemplatepropertiesDesc;

    private String ruletemplatepropertiesIsMandatory;

    private String ruletemplatepropertiesCreatedBy;

    private String ruletemplatepropertiesUpdatedBy;

    private String ruletemplatepropertiesCreatedDate;

    private String ruletemplatepropertiesUpdatedDate;
}
