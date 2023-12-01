package com.cv.dataqualityapi.rest.impl;

import com.cv.dataqualityapi.rest.RuleEntityMapRest;
import com.cv.dataqualityapi.service.RuleEntityMapService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RuleEntityMapRestImpl implements RuleEntityMapRest {

    @Autowired
    private RuleEntityMapService ruleEntityMapService;
}
