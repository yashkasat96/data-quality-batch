package com.cv.dataqualityapi.Repo;

import com.cv.dataqualityapi.model.RuleProperties;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface RulePropertiesRepo extends JpaRepository<RuleProperties,Integer> {

}
