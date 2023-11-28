package com.cv.dataqualityapi.service.impl;

import com.cv.dataqualityapi.Repo.RulePropertiesRepo;
import com.cv.dataqualityapi.model.RuleProperties;
import com.cv.dataqualityapi.service.RulePropertiesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class RulePropertiesServiceImpl implements RulePropertiesService {
    @Autowired
    private RulePropertiesRepo rulePropertiesRepo;

    public RuleProperties saveAllRuleProperties(List<RuleProperties> rulePropertiesList){
        return (RuleProperties) rulePropertiesRepo.saveAll(rulePropertiesList);
    }

    public RuleProperties saveRuleProperties(RuleProperties ruleProperties){
        return rulePropertiesRepo.save(ruleProperties);
    }

    public Optional<RuleProperties> getRulePropertiesById(int id){
        return rulePropertiesRepo.findById(id);
    }

    public RuleProperties getAllRuleProperties(){
        return (RuleProperties) rulePropertiesRepo.findAll();
    }

    public String deleteRuleProperties(int id){
        rulePropertiesRepo.deleteById(id);
        return "Delete Properties" + id;
    }

}
