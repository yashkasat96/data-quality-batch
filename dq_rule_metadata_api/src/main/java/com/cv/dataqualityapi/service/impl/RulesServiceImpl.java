package com.cv.dataqualityapi.service.impl;

import com.cv.dataqualityapi.Repo.RulesRepo;
import com.cv.dataqualityapi.model.Rules;
import com.cv.dataqualityapi.service.RulesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class RulesServiceImpl implements RulesService {
    @Autowired
    private RulesRepo rulesRepo;

    public String deleteRulesById(int id){
        rulesRepo.deleteById(id);
        return "The Rule Id has been deleted "+id ;
    }

    public Optional<Rules> getRulesById(int id){
        return rulesRepo.findById(id);
    }

    public Rules getAllRules(){
        return (Rules) rulesRepo.findAll();
    }

    public Rules saveAllRules(List<Rules> rules){
        return (Rules) rulesRepo.saveAll(rules);
    }

    public Rules saveRules(Rules rules){
        return rulesRepo.save(rules);
    }

    public Optional<Rules> getRulesByName(String name){
        return rulesRepo.findByRuleName(name);
    }
}
