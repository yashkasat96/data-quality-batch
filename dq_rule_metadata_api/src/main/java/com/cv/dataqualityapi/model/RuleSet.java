package com.cv.dataqualityapi.model;

import java.util.Set;

import javax.persistence.*;

import com.fasterxml.jackson.annotation.JsonBackReference;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

//@NamedQueries({
//		@NamedQuery(name = "RuleSet.getRuleSetByRuleSetName", query = "SELECT rs from RuleSet rs where upper(rs.rulesetName) = upper(:rulesetName)"),
//		@NamedQuery(name = "RuleSet.getRuleSetCount", query = "SELECT rs from RuleSet rs where upper(rs.rulesetName) = upper(:rulesetName) and upper(rs.rulesetDesc) = upper(:rulesetDesc)") })

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name = "ruleset")
public class RuleSet {

	@Id
	//@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "rule_set_id")
	private Integer rulesetId;

	@Column(name = "rule_set_name", nullable = false, length = 100)
	private String rulesetName;

	@Column(name = "rule_set_desc", nullable = false, length = 200)
	private String rulesetDesc;

	@Column(name = "notification_email", nullable = false, length = 200)
	private String rulesetNotificationEmail;

	@Column(name = "created_by", nullable = false, length = 200)
	private String rulesetCreatedBy;

	@Column(name = "created_date", nullable = false, length = 200)
	private String rulesetCreatedDate;

	@Column(name = "updated_by", nullable = false, length = 200)
	private String rulesetUpdatedBy;

	@Column(name = "updated_date", nullable = false, length = 200)
	private String rulesetUpdatedDate;

	@OneToMany(mappedBy = "ruleSet",fetch = FetchType.LAZY)
	private Set<Rules> rules;
}

