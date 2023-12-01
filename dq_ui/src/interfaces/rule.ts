interface RuleProperty {
  propertyName: string;
  propertyValue: string;
}

export interface RuleFormData {
  id: string;
  ruleTemplateName: string;
  dqMetric: string;
  ruleName: string;
  ruleDescription: string;
  ruleSetName: string;
  primarySourceEntity: string;
  isDraft: boolean;
  primaryTargetEntity: string;
  secondarySourceEntity: string;
  secondaryTargetEntity: string;
  properties: RuleProperty[];
}
