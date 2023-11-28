package com.cv.dataqualityapi.dto;

import java.util.Properties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UpdateRulesDto {

	private Integer ruleId;

	private String rulesType;

	private String ruleDesc;

	private String ruleName;

	private String entityTable;

	private Properties properties;

	private String ruleSet;
}
