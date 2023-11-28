package com.cv.dataqualityapi.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.util.Set;

@Entity
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "entity_properties")
public class EntityProperties {

	@Id
	//@GeneratedValue(strategy = GenerationType.IDENTITY)
	@ApiModelProperty(notes = "Entity Id", example = "1", required = true)
	@Column(name = "entity_prop_id")
	private Integer entitypropId;

	@ApiModelProperty(notes = "Entity Id", example = "1", required = true)
	@Column(name = "entity_id")
	private Integer EntityId;

	@ApiModelProperty(notes = "entity_prop_key", example = "Source")
	@Column(name = "entity_prop_key")
	private String entitypropKey;

	@ApiModelProperty(notes = "entity_prop_value", example = "Source")
	@Column(name = "entity_prop_value")
	private String entitypropValue;

	@ApiModelProperty(notes = "created_by", example = "CSV")
	@Column(name = "created_by")
	private String entitypropCreatedBy;

	@ApiModelProperty(notes = "updated_by", example = "FILE")
	@Column(name = "updated_by")
	private String entitypropUpdatedBy;

	@ApiModelProperty(notes = "created_date", example = "CSV")
	@Column(name = "created_date")
	private String entitypropCreatedDate;

	@ApiModelProperty(notes = "updated_date", example = "CSV")
	@Column(name = "updated_date")
	private String entitypropUpdatedDate;

	@ManyToOne()
	@JoinColumn(name = "entity_id", insertable = false, updatable = false)
	private Entities entities;

}