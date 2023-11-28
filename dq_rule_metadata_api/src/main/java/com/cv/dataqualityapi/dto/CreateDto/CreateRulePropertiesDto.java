package com.cv.dataqualityapi.dto.CreateDto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateRulePropertiesDto {

    private Integer rulepropertiesId;

    private Integer rulepropertiesRuleId;

    private String rulepropertiesKey;

    private String rulepropertiesValue;

    private String rulepropertiesCreatedBy;

    private String rulepropertiesUpdatedBy;

    private String rulepropertiesCreatedDate;

    private String rulepropertiesUpdatedDate;

}
