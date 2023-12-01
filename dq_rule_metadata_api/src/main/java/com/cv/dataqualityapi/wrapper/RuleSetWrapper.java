
package com.cv.dataqualityapi.wrapper;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter

@Setter

@NoArgsConstructor

@ToString
public class RuleSetWrapper {

	private Integer rulesetId;

	private String rulesetName;
 
	private String rulesetDesc;

	private String rulesetNotificationPreferences;

}
