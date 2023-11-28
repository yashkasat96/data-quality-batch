package com.cv.dataqualityapi.dto.CreateDto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateRuleEntityMapDto {

    private Integer ruleEntityMapId;

    private Integer ruleEntityMapRuleId;

    private Integer entityId;

    private String ruleEntityMapEntityBehaviour;

    private String ruleEntityMapIsPrimary;

    private String ruleEntityMapCreatedBy;

    private String ruleEntityMapUpdatedBy;

    private String ruleEntityMapCreatedDate;

    private String ruleEntityMapUpdatedDate;
}