package com.cv.dataqualityapi.model;

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
@Table(name = "rule_properties")
public class RuleProperties {

    @Id
   // @GeneratedValue(strategy = GenerationType.IDENTITY)
    @ApiModelProperty(notes = "rule_prop_id", example = "1", required = true)
    @Column(name = "rule_prop_id",nullable = false, length = 200)
    private Integer rulepropertiesId;

    @ApiModelProperty(notes = "rule_id", example = "1", required = true)
    @Column(name = "rule_id",nullable = false, length = 200)
    private Integer rulepropertiesRuleId;

    @ApiModelProperty(notes = "rule_properties_key", example = "1", required = true)
    @Column(name = "rule_prop_key",nullable = false, length = 200)
    private String rulepropertiesKey;

    @ApiModelProperty(notes = "rule_properties_value", example = "1", required = true)
    @Column(name = "rule_prop_value",nullable = false, length = 200)
    private String rulepropertiesValue;

    @ApiModelProperty(notes = "created_by", example = "CSV")
    @Column(name = "created_by")
    private String rulepropertiesCreatedBy;

    @ApiModelProperty(notes = "updated_by", example = "FILE")
    @Column(name = "updated_by")
    private String rulepropertiesUpdatedBy;

    @ApiModelProperty(notes = "created_date", example = "CSV")
    @Column(name = "created_date")
    private String rulepropertiesCreatedDate;

    @ApiModelProperty(notes = "updated_date", example = "CSV")
    @Column(name = "updated_date")
    private String rulepropertiesUpdatedDate;

    @ManyToOne
    @MapsId("rule_id")
    @JoinColumn(name = "rule_id", insertable = false, updatable = false)
    private Rules rules;
}
