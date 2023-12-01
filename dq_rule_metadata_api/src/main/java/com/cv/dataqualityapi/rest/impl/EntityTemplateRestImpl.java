package com.cv.dataqualityapi.rest.impl;

import com.cv.dataqualityapi.rest.EntityTemplateRest;
import com.cv.dataqualityapi.service.EntityTemplateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EntityTemplateRestImpl implements EntityTemplateRest {

    @Autowired
    private EntityTemplateService entityTemplateService;
}

