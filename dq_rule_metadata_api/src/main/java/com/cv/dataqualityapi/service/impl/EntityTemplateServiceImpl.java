package com.cv.dataqualityapi.service.impl;

import com.cv.dataqualityapi.Repo.EntityTemplateRepo;
import com.cv.dataqualityapi.service.EntityTemplateService;
import org.springframework.beans.factory.annotation.Autowired;
import com.cv.dataqualityapi.model.EntityTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class EntityTemplateServiceImpl implements EntityTemplateService {
    @Autowired
    private EntityTemplateRepo entityTemplateRepo;

    public EntityTemplate saveEntityTemplate(EntityTemplate entityTemplate){
        return entityTemplateRepo.save(entityTemplate);
    }

    public EntityTemplate saveAllEntityTemplate(List<EntityTemplate> entityTemplateList){
        return (EntityTemplate) entityTemplateRepo.saveAll(entityTemplateList);
    }

    public Optional<EntityTemplate> getEntityTemplateById(int id){
        return entityTemplateRepo.findById(id);
    }

    public EntityTemplate getAllEntityTemplate(){
        return (EntityTemplate) entityTemplateRepo.findAll();
    }

    public String deleteEntityTemplate(int id){
        entityTemplateRepo.deleteById(id);
        return "Entity Template Delete" + id ;
    }
}
