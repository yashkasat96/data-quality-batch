package com.cv.dataqualityapi.Repo;

import com.cv.dataqualityapi.model.RuleSet;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface RuleSetRepo extends JpaRepository<RuleSet,Integer> {

   Optional<RuleSet> findByRulesetName(String rulesetName);
}
