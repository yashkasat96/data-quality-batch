package com.cv.dataqualityapi.rest.impl;

import com.cv.dataqualityapi.rest.RulePropertiesRest;
import com.cv.dataqualityapi.service.RulePropertiesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RulePropertiesRestImpl implements RulePropertiesRest {

    @Autowired
    private RulePropertiesService rulePropertiesService;
}
