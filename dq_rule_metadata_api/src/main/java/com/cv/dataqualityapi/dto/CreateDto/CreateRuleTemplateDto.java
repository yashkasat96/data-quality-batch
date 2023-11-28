package com.cv.dataqualityapi.dto.CreateDto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateRuleTemplateDto {

	private Integer ruletemplateId;

	private String ruletemplateDqMetric;

	private String ruletemplateName;

	private String ruletemplateDesc;

	private String ruletemplateCreatedBy;

	private String ruletemplateUpdatedBy;

	private String ruletemplateCreatedDate;

	private String ruletemplateUpdatedDate;
}