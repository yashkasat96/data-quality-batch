package com.cv.dataqualityapi.service.impl;

import com.cv.dataqualityapi.Repo.RuleEntityMapRepo;
import com.cv.dataqualityapi.model.RuleEntityMap;
import com.cv.dataqualityapi.service.RuleEntityMapService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class RuleEntityMapServiceImpl implements RuleEntityMapService {

    @Autowired
    private RuleEntityMapRepo ruleEntityMapRepo;

    public RuleEntityMap saveAllRuleEntityMap(List<RuleEntityMap> ruleEntityMaps){
        return (RuleEntityMap) ruleEntityMapRepo.saveAll(ruleEntityMaps);
    }

    public RuleEntityMap saveRuleEntityMap(RuleEntityMap ruleEntityMap){
        return ruleEntityMapRepo.save(ruleEntityMap);
    }

    public Optional<RuleEntityMap> getRuleEntityMapById(int id){
        return ruleEntityMapRepo.findById(id);
    }

    public RuleEntityMap getAllRuleEntityMap(){
        return (RuleEntityMap) ruleEntityMapRepo.findAll();
    }

    public String deleteRuleEntityMapById(int id){
        ruleEntityMapRepo.deleteById(id);
        return "deleted Rule Entity Map"+ id ;
    }

}
