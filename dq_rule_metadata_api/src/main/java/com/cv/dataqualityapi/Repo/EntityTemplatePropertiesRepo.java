package com.cv.dataqualityapi.Repo;

import com.cv.dataqualityapi.model.EntityTemplateProperties;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface EntityTemplatePropertiesRepo extends JpaRepository<EntityTemplateProperties,Integer>{

}
