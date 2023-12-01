package com.cv.dataqualityapi.rest.impl;

import com.cv.dataqualityapi.rest.RuleTemplatePropertiesRest;
import com.cv.dataqualityapi.service.RuleTemplatePropertiesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RuleTemplatePropertiesRestImpl implements RuleTemplatePropertiesRest {

    @Autowired
    private RuleTemplatePropertiesService ruleTemplatePropertiesService;
}
