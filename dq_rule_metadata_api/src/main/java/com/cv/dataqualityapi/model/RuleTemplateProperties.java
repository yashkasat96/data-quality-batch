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
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "rule_template_properties")
public class RuleTemplateProperties {

    @Id
   // @GeneratedValue(strategy = GenerationType.IDENTITY)
    @ApiModelProperty(notes = "rule_template_prop_id", example = "1", required = true)
    @Column(name = "rule_template_prop_id",nullable = false, length = 200)
    private Integer ruletemplatepropertiesId;

    @Column(name = "rule_template_id")
    @ApiModelProperty(notes = "rule_template_id")
    private Integer ruletemplateId;

    @ApiModelProperty(notes = "rule_template_prop_key", example = "1", required = true)
    @Column(name = "rule_template_prop_key",nullable = false, length = 200)
    private String ruletemplatepropertiesKey;

    @ApiModelProperty(notes = "rule_template_prop_type", example = "1", required = true)
    @Column(name = "rule_template_prop_type",nullable = false, length = 200)
    private String ruletemplatepropertiesType;

    @ApiModelProperty(notes = "rule_template_prop_value", example = "1", required = true)
    @Column(name = "rule_template_prop_value",nullable = false, length = 200)
    private String ruletemplatepropertiesValue;

    @ApiModelProperty(notes = "rule_template_prop_desc", example = "1", required = true)
    @Column(name = "rule_template_prop_desc",nullable = false, length = 200)
    private String ruletemplatepropertiesDesc;

    @ApiModelProperty(notes = "is_mandatory", example = "CSV")
    @Column(name = "is_mandatory")
    private String ruletemplatepropertiesIsMandatory;

    @ApiModelProperty(notes = "created_by", example = "CSV")
    @Column(name = "created_by")
    private String ruletemplatepropertiesCreatedBy;

    @ApiModelProperty(notes = "updated_by", example = "FILE")
    @Column(name = "updated_by")
    private String ruletemplatepropertiesUpdatedBy;

    @ApiModelProperty(notes = "created_date", example = "CSV")
    @Column(name = "created_date")
    private String ruletemplatepropertiesCreatedDate;

    @ApiModelProperty(notes = "updated_date", example = "CSV")
    @Column(name = "updated_date")
    private String ruletemplatepropertiesUpdatedDate;

    @ManyToOne(fetch = FetchType.LAZY)
    @MapsId("rule_template_id")
    @JoinColumn(name = "rule_template_id",insertable = false,updatable = false)
    private RuleTemplate ruleTemp;
}
