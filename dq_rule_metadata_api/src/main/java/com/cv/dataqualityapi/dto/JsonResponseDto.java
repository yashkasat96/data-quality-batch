package com.cv.dataqualityapi.dto;

import java.util.List;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class JsonResponseDto {

  private Integer ruleset_id;

  private String ruleset_name;

  private String ruleset_desc;

  private List<String> notification_preference;

  private List<RulesJsonDto> rules;
}