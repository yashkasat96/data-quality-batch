package com.cv.dataqualityapi.model;
import javax.persistence.*;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Set;


@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "rule_template")
public class RuleTemplate {

    @Id
   // @GeneratedValue(strategy = GenerationType.IDENTITY)
    @ApiModelProperty(notes = "rule_template_id", example = "1", required = true)
    @Column(name = "rule_template_id",nullable = false, length = 200)
    private Integer ruletemplateId;

    @ApiModelProperty(notes = "dq_metric", example = "1", required = true)
    @Column(name = "dq_metric",nullable = false, length = 200)
    private String ruletemplateDqMetric;

    @ApiModelProperty(notes = "rule_template_name", example = "1", required = true)
    @Column(name = "rule_template_name",nullable = false, length = 200)
    private String ruletemplateName;

    @ApiModelProperty(notes = "rule_template_desc", example = "1", required = true)
    @Column(name = "rule_template_desc",nullable = false, length = 200)
    private String ruletemplateDesc;

    @ApiModelProperty(notes = "created_by", example = "CSV")
    @Column(name = "created_by")
    private String ruletemplateCreatedBy;

    @ApiModelProperty(notes = "updated_by", example = "FILE")
    @Column(name = "updated_by")
    private String ruletemplateUpdatedBy;

    @ApiModelProperty(notes = "created_date", example = "CSV")
    @Column(name = "created_date")
    private String ruletemplateCreatedDate;

    @ApiModelProperty(notes = "updated_date", example = "CSV")
    @Column(name = "updated_date")
    private String ruletemplateUpdatedDate;

    @OneToMany(mappedBy = "ruleTemp", cascade = CascadeType.ALL,fetch = FetchType.LAZY)
    private Set<RuleTemplateProperties> ruleTempProp;

    @OneToMany(mappedBy = "ruleTemp", cascade = CascadeType.ALL,fetch = FetchType.LAZY)
    private Set<Rules> rules;
}
