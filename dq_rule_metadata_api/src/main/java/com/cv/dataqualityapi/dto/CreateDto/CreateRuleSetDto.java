package com.cv.dataqualityapi.dto.CreateDto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateRuleSetDto {

	private Integer rulesetId;

	private String rulesetName;

	private String rulesetDesc;

	private String rulesetNotificationPreferences;

	private String rulesetCreatedBy;

	private String rulesetCreatedDate;

	private String rulesetUpdatedBy;
}