package com.cv.dataqualityapi.Repo;

import com.cv.dataqualityapi.model.RuleTemplateProperties;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RuleTemplatePropertiesRepo extends JpaRepository<RuleTemplateProperties,Integer> {
}
