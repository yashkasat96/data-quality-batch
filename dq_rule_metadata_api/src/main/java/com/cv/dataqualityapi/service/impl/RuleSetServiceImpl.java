package com.cv.dataqualityapi.service.impl;

import com.cv.dataqualityapi.Repo.RuleSetRepo;
import com.cv.dataqualityapi.model.RuleSet;
import com.cv.dataqualityapi.service.RulesSetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class RuleSetServiceImpl implements RulesSetService {

    @Autowired
    private RuleSetRepo ruleSetRepository;

    public RuleSet saveRuleSet(RuleSet ruleSet){
        return ruleSetRepository.save(ruleSet);
    }

    public RuleSet saveRuleSets(List<RuleSet> ruleSets){
        return (RuleSet) ruleSetRepository.saveAll(ruleSets);
    }

    public List<RuleSet> getRuleSet(){
        return ruleSetRepository.findAll();
    }

    public List<RuleSet> getRuleSetById(int id){
        return (List<RuleSet>) ruleSetRepository.findById(id).orElse(null);
    }

    public Optional<RuleSet> getRuleSetByName(String name){
        return ruleSetRepository.findByRulesetName(name);
    }

    public String deleteRuleSet(int id){
        ruleSetRepository.deleteById(id);
        return "RuleSet Deleted" + id;
    }

    public RuleSet updateRuleSet(RuleSet ruleSet){
        RuleSet existingRuleSet = ruleSetRepository.findById(ruleSet.getRulesetId()).orElse(null);
        existingRuleSet.setRulesetName(ruleSet.getRulesetName());
        //existingRuleSet.getRulesetDesc(ruleSet.getRulesetDesc());
        existingRuleSet.setRulesetUpdatedBy(ruleSet.getRulesetUpdatedBy());
        existingRuleSet.setRulesetUpdatedDate(ruleSet.getRulesetUpdatedDate());
        return ruleSetRepository.save(existingRuleSet);
    }
}