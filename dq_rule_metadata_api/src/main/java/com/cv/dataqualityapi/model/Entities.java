package com.cv.dataqualityapi.model;

import javax.persistence.*;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Set;

@Entity
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "entity")
public class Entities {

	@Id
	//@GeneratedValue(strategy = GenerationType.IDENTITY)
	@ApiModelProperty(notes = "Entity Id", example = "1", required = true)
	@Column(name = "entity_id")
	private Integer entityId;

	@ApiModelProperty(notes = "entity_template_id", example = "Source")
	@Column(name = "entity_template_id",nullable = false, length = 200)
	private Integer entitytemplateId;

	@ApiModelProperty(notes = "entity_physical_name", example = "FILE")
	@Column(name = "entity_physical_name",nullable = false, length = 200)
	private String entityPhysicalName;

	@ApiModelProperty(notes = "entity_name", example = "CSV")
	@Column(name = "entity_name",nullable = false, length = 200)
	private String entityName;

	@ApiModelProperty(notes = "entity_primary_key", example = "FILE")
	@Column(name = "entity_primary_key",nullable = false, length = 200)
	private String entityPrimaryKey;

	@ApiModelProperty(notes = "created_by", example = "CSV")
	@Column(name = "created_by",nullable = false, length = 200)
	private String createdBy;

	@ApiModelProperty(notes = "updated_by", example = "FILE")
	@Column(name = "updated_by",nullable = false, length = 200)
	private String updatedBy;

	@ApiModelProperty(notes = "created_date", example = "CSV")
	@Column(name = "created_date",nullable = false, length = 200)
	private String createdDate;

	@ApiModelProperty(notes = "updated_date", example = "CSV")
	@Column(name = "updated_date",nullable = false, length = 200)
	private String updatedDate;

	@ManyToOne
	@MapsId("entity_template_id")
	@JoinColumn(name = "entity_template_id",insertable = false, updatable = false)
	private EntityTemplate entityTemp;

	@OneToMany(mappedBy = "entities"  , cascade = CascadeType.ALL)
	private Set<EntityProperties> entityProp;

	@OneToMany(mappedBy = "entities", cascade = CascadeType.ALL)
	private Set<RuleEntityMap> ruleEntityMap;
}