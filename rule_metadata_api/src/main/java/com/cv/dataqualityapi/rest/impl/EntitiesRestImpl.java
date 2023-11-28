package com.cv.dataqualityapi.rest.impl;

import com.cv.dataqualityapi.rest.EntitiesRest;
import com.cv.dataqualityapi.service.EntitiesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EntitiesRestImpl implements EntitiesRest {

    @Autowired
    private EntitiesService entitiesService;
}
