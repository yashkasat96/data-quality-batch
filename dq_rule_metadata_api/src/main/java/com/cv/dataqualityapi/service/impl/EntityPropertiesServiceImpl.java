package com.cv.dataqualityapi.service.impl;

import com.cv.dataqualityapi.Repo.EntityPropertiesRepo;
import com.cv.dataqualityapi.model.Entities;
import com.cv.dataqualityapi.model.EntityProperties;
import com.cv.dataqualityapi.service.EntityPropertiesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EntityPropertiesServiceImpl implements EntityPropertiesService {

    @Autowired
    private EntityPropertiesRepo entityPropertiesRepo;

    public EntityProperties saveEntitiesProp(List<EntityProperties> entitiesProperties){
        return (EntityProperties) entityPropertiesRepo.saveAll(entitiesProperties);
    }

    public EntityProperties saveEntityProp(EntityProperties entityProperties){
        return entityPropertiesRepo.save(entityProperties);
    }

    public EntityProperties getEntityPropbyId(int id){
        return (EntityProperties) entityPropertiesRepo.findByEntitypropId(id).orElse(null);

    }

    public EntityProperties getAllEntityProp(){
        return (EntityProperties) entityPropertiesRepo.findAll();
    }

}
