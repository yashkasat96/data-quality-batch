package com.cv.dataqualityapi.service;

import com.cv.dataqualityapi.dto.JsonResponseDto;

public interface GenerateRulesJsonService {
	JsonResponseDto generateRulesJson(String ruleSetName);
}
