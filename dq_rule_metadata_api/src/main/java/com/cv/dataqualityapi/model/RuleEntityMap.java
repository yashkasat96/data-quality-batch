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
@Table(name = "rule_entity_map")
public class RuleEntityMap {

    @Id
  //  @GeneratedValue(strategy = GenerationType.IDENTITY)
    @ApiModelProperty(notes = "rule_entity_map_id", example = "1", required = true)
    @Column(name = "rule_entity_map_id",nullable = false, length = 200)
    private Integer ruleEntityMapId;

    @ApiModelProperty(notes = "rule_id", example = "1", required = true)
    @Column(name = "rule_id",nullable = false, length = 200)
    private Integer ruleEntityMapRuleId;

    @ApiModelProperty(notes = "entity_id", example = "1", required = true)
    @Column(name = "entity_id",nullable = false, length = 200)
    private Integer entityId;

    @ApiModelProperty(notes = "entity_behaviour", example = "1", required = true)
    @Column(name = "entity_behaviour",nullable = false, length = 200)
    private String ruleEntityMapEntityBehaviour;

    @ApiModelProperty(notes = "is_primary", example = "CSV")
    @Column(name = "is_primary")
    private String ruleEntityMapIsPrimary;

    @ApiModelProperty(notes = "created_by", example = "CSV")
    @Column(name = "created_by")
    private String ruleEntityMapCreatedBy;

    @ApiModelProperty(notes = "updated_by", example = "FILE")
    @Column(name = "updated_by")
    private String ruleEntityMapUpdatedBy;

    @ApiModelProperty(notes = "created_date", example = "CSV")
    @Column(name = "created_date")
    private String ruleEntityMapCreatedDate;

    @ApiModelProperty(notes = "updated_date", example = "CSV")
    @Column(name = "updated_date")
    private String ruleEntityMapUpdatedDate;

    @ManyToOne(fetch = FetchType.LAZY)
    @MapsId("entity_id")
    @JoinColumn(name = "entity_id", insertable = false, updatable = false)
    private Entities entities;

    @ManyToOne(fetch = FetchType.LAZY)
    @MapsId("rule_id")
    @JoinColumn(name = "rule_id", insertable = false, updatable = false)
    @JsonBackReference
    private Rules rules;
}
