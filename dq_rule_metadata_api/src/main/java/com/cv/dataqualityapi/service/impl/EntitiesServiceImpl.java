package com.cv.dataqualityapi.service.impl;

import com.cv.dataqualityapi.Repo.EntityRepo;
import com.cv.dataqualityapi.model.Entities;
import com.cv.dataqualityapi.service.EntitiesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EntitiesServiceImpl implements EntitiesService {
    @Autowired
    private EntityRepo entityRepo;

    public Entities saveEntities(List<Entities> entities){
        return (Entities) entityRepo.saveAll(entities);
    }

    public Entities saveEntity(Entities entity){
        return entityRepo.save(entity);
    }

    public Entities getEntities(){
        return (Entities) entityRepo.findAll();
    }

    public Entities getEntityById(int id){
        return entityRepo.findByEntityId(id).orElse(null);
    }

    public Entities getEntityByName(String name){
        return (Entities) entityRepo.findByEntityName(name).orElse(null);
    }

}
