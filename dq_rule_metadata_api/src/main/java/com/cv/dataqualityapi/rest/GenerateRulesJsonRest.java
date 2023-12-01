package com.cv.dataqualityapi.rest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import com.cv.dataqualityapi.dto.JsonResponseDto;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RequestMapping("/generate-rules-json")
public interface GenerateRulesJsonRest {
	@ApiOperation(value = "generates rules json", notes = "Returns generated json for rules", response = JsonResponseDto.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Success"),
			@ApiResponse(code = 404, message = "Not found - The url was not found") })
	@GetMapping("/{ruleSetName}")
	JsonResponseDto generateRulesJson(@PathVariable String ruleSetName);
}
