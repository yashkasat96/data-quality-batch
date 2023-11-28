package com.cv.dataqualityapi.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EntityDetailsDto {

	private String behaviour;

	private String type;

	private String subType;

	private String tableName;

	private String location;

	private String description;

	private String primaryKey;
}
