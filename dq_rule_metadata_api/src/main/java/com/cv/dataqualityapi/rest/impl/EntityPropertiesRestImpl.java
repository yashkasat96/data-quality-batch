package com.cv.dataqualityapi.rest.impl;

import com.cv.dataqualityapi.rest.EntityPropertiesRest;
import com.cv.dataqualityapi.service.EntityPropertiesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EntityPropertiesRestImpl implements EntityPropertiesRest {

    @Autowired
    private EntityPropertiesService entityPropertiesService;

}
