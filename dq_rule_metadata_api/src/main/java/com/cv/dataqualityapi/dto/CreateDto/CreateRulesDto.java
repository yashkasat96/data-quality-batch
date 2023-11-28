package com.cv.dataqualityapi.dto.CreateDto;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateRulesDto {

	private Integer ruleId;

	private Integer rulesetId;

	private Integer ruletemplateId;

	private String ruleDesc;

	private String ruleName;

	private Date createdDate;

	private Date updatedDate;

	private String createdBy;

	private String updatedBy;
}