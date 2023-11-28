package com.cv.dataqualityapi.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class DataEntityAssociations {

	private Integer entity_id;

	private String entity_name;

	private String entity_physical_name;

	private String entity_behaviour;

	private String entity_type;

	private String entity_sub_type;

	private String primary_key;

	private String is_primary;

	private List<EntityPropertiesDto> properties;

	private List<EntityTemplatePropertiesDto> all_entity_properties;
}