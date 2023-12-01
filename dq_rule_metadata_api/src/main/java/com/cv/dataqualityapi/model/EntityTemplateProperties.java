package com.cv.dataqualityapi.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.util.Set;

@Setter
@Entity
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "entity_template_properties")

public class EntityTemplateProperties {

    @Id
   // @GeneratedValue(strategy = GenerationType.IDENTITY)
    @ApiModelProperty(notes = "entitytemplateprop Id", example = "1", required = true)
    @Column(name = "entity_template_prop_id")
    private Integer entitytemplatepropId;

    @ApiModelProperty(notes = "entity_template_id")
    @Column(name = "entity_template_id")
    private Integer entitytemplateId;

    @ApiModelProperty(notes = "entity_template_prop_key", example = "Source")
    @Column(name = "entity_template_prop_key")
    private String entitytemplatepropKey;

    @ApiModelProperty(notes = "entity_template_prop_desc", example = "Source")
    @Column(name = "entity_template_prop_desc")
    private String entitytemplatepropDesc;

    @ApiModelProperty(notes = "entity_template_prop_type", example = "Source")
    @Column(name = "entity_template_prop_type")
    private String entitytemplatepropType;

    @ApiModelProperty(notes = "is_mandatory", example = "Source")
    @Column(name = "is_mandatory")
    private String isMandatory;

    @ApiModelProperty(notes = "created_by", example = "Source")
    @Column(name = "created_by")
    private String entitytemplatepropCreatedBy;

    @ApiModelProperty(notes = "created_date", example = "Source")
    @Column(name = "created_date")
    private String entitytemplatepropCreatedDate;

    @ApiModelProperty(notes = "updated_by", example = "Source")
    @Column(name = "updated_by")
    private String entitytemplatepropUpdatedBy;

    @ApiModelProperty(notes = "updated_date", example = "Source")
    @Column(name = "updated_date")
    private String entitytemplatepropUpdatedDate;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "entity_template_id", insertable = false, updatable = false)
    @JsonBackReference
    private EntityTemplate entityTemp;

}