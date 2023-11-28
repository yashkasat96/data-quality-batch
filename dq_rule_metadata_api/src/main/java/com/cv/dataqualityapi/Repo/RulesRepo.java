package com.cv.dataqualityapi.Repo;

import com.cv.dataqualityapi.model.Rules;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public interface RulesRepo extends JpaRepository<Rules,Integer> {
        Optional<Rules> findByRuleName(String ruleName);

}

