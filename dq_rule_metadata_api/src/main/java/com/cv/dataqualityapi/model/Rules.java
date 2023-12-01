package com.cv.dataqualityapi.model;

import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.persistence.*;

import com.cv.dataqualityapi.converter.PropertiesConverter;
import com.fasterxml.jackson.annotation.*;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

//@NamedQueries({
//		@NamedQuery(name = "Rules.getAllRulesPks", query = "SELECT new com.cv.dataqualityapi.model.Rules(r.ruleId) from Rules r"),
		// @NamedQuery(name = "Rules.getRuleCount", query = "SELECT count(r) from Rules
		// r JOIN r.clients c JOIN r.rulesType rt where UPPER(rt.typeName) =
		// UPPER(:typeName) and UPPER(r.ruleDesc) = UPPER(:ruleDesc) and
		// UPPER(r.tableName) = UPPER(:tableName) and UPPER(r.columnName) =
		// UPPER(columnName) and UPPER(r.columnValue) = UPPER(:columnValue) and
		// UPPER(r.sourceName) = UPPER(sourceName) and UPPER(c.clientName) =
		// UPPER(:clientName)"),
//		@NamedQuery(name = "Rules.getRules", query = "SELECT r from Rules r JOIN r.rulesType rt JOIN r.ruleSet rs JOIN r.entityTable et where UPPER(rt.typeName) = UPPER(:typeName) or UPPER(r.ruleDesc) = UPPER(:ruleDesc) or UPPER(r.ruleName) = UPPER(:ruleName) or  UPPER(rs.rulesetName) = UPPER(:ruleSet) or UPPER(et.tableName) = UPPER(:entityTable)") })

@ApiModel
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name = "rules")
public class Rules {

	@Id
	//@GeneratedValue(strategy = GenerationType.IDENTITY)
	@ApiModelProperty(notes = "Rule ID", example = "1", required = true)
	@Column(name = "rule_id")
	private Integer ruleId;

	@Column(name = "rule_set_id")
	@ApiModelProperty(notes = "rule_set_id")
	private Integer rulesetId;

	@Column(name = "rule_template_id")
	@ApiModelProperty(notes = "rule_template_id")
	private Integer ruletemplateId;

	// newly added
	@Column(name = "rule_desc", nullable = false, length = 100)
	@ApiModelProperty(notes = "Rule Description", example = "Some description of the Rule", required = false)
	private String ruleDesc;

	@Column(name = "rule_name", nullable = false)
	@ApiModelProperty(notes = "Rule Name", example = "Some Name for Rule", required = false)
	private String ruleName;

	@Column(name = "created_date")
	@ApiModelProperty(notes = "Rule creation timestamp", example = "2023-04-18 14:20:20.785", required = false)
	@Temporal(TemporalType.TIMESTAMP)
	private Date createdDate;

	@Column(name = "updated_date")
	@ApiModelProperty(notes = "Rule updation timestamp", example = "2023-04-18 14:20:20.785", required = false)
	@Temporal(TemporalType.TIMESTAMP)
	private Date updatedDate;

	@Column(name = "created_by", length = 100)
	@ApiModelProperty(notes = "Created by name", example = "Clairvoyant", required = false)
	private String createdBy;

	@Column(name = "updated_by", length = 100)
	@ApiModelProperty(notes = "Updated by name", example = "EXL", required = false)
	private String updatedBy;

	@ManyToOne
	@MapsId("rule_set_id")
	@JoinColumn(name = "rule_set_id", insertable = false, updatable = false)
	private RuleSet ruleSet;

	@ManyToOne
	@MapsId("rule_template_id")
	@JoinColumn(name = "rule_template_id", insertable = false, updatable = false)
	private RuleTemplate ruleTemp;

	@OneToMany(mappedBy = "rules")
	private Set<RuleProperties> rulesprop;

	@OneToMany(mappedBy = "rules", cascade = CascadeType.ALL)
	private Set<RuleEntityMap> ruleEntityMap;

	public Rules(Integer ruleId) {
		super();
		this.ruleId = ruleId;
	}
}