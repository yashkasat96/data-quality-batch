package com.cv.dataqualityapi.rest.impl;

import com.cv.dataqualityapi.rest.EntityPropertiesRest;
import com.cv.dataqualityapi.rest.EntityTemplatePropertiesRest;
import com.cv.dataqualityapi.service.EntityTemplatePropertiesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EntityTemplatePropertiesRestImpl implements EntityTemplatePropertiesRest {

    @Autowired
    private EntityTemplatePropertiesService entityTemplatePropertiesService;
}
