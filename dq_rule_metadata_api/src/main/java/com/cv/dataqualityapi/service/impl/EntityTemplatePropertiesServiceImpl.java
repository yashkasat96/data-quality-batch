package com.cv.dataqualityapi.service.impl;

import com.cv.dataqualityapi.Repo.EntityTemplatePropertiesRepo;
import com.cv.dataqualityapi.model.EntityTemplateProperties;
import com.cv.dataqualityapi.service.EntityTemplatePropertiesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class EntityTemplatePropertiesServiceImpl implements EntityTemplatePropertiesService {

    @Autowired
    private EntityTemplatePropertiesRepo entityTemplatePropertiesRepo;

    public EntityTemplateProperties saveAllEntityTemplateProps(List<EntityTemplateProperties> entityTemplateProperties){
        return (EntityTemplateProperties) entityTemplatePropertiesRepo.saveAll(entityTemplateProperties);
    }

    public EntityTemplateProperties saveEntityTemplateProps(EntityTemplateProperties entityTemplateProperties){
        return entityTemplatePropertiesRepo.save(entityTemplateProperties);
    }

    public EntityTemplateProperties getAllEntityTemplateProps(){
        return (EntityTemplateProperties) entityTemplatePropertiesRepo.findAll();
    }

    public Optional<EntityTemplateProperties> getEntityTemplatePropsById(int id){
        return Optional.ofNullable(entityTemplatePropertiesRepo.findById(id).orElse(null));
    }

    public String deleteEntityTemplateProps(int id){
        entityTemplatePropertiesRepo.deleteById(id);
        return "Entity Template Properties " + id ;
    }
}
