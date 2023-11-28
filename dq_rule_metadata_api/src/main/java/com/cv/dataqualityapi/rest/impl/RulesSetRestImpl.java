package com.cv.dataqualityapi.rest.impl;

import com.cv.dataqualityapi.rest.RulesSetRest;
import org.springframework.beans.factory.annotation.Autowired;
import com.cv.dataqualityapi.service.RulesSetService;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RulesSetRestImpl implements RulesSetRest {

    @Autowired
    private RulesSetService rulesSetService;

}
