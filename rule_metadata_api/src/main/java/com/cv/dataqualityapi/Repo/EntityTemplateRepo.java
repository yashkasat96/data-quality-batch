package com.cv.dataqualityapi.Repo;

import com.cv.dataqualityapi.model.EntityTemplate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EntityTemplateRepo extends JpaRepository <EntityTemplate, Integer> {

}
