package com.cv.dataqualityapi.rest.impl;

import com.cv.dataqualityapi.rest.RuleTemplateRest;
import com.cv.dataqualityapi.service.RuleTemplateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RuleTemplateRestImpl implements RuleTemplateRest {

    @Autowired
    private RuleTemplateService ruleTemplateService;
}
