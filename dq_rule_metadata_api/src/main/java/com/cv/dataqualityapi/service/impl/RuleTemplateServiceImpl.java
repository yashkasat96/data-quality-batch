package com.cv.dataqualityapi.service.impl;

import com.cv.dataqualityapi.Repo.RuleTemplateRepo;
import com.cv.dataqualityapi.model.RuleTemplate;
import com.cv.dataqualityapi.service.RuleTemplateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class RuleTemplateServiceImpl implements RuleTemplateService {
    @Autowired
    private RuleTemplateRepo ruleTemplateRepo;

    public List<RuleTemplate> getAllRuleTemplate(){
        return ruleTemplateRepo.findAll();
    }

    public Optional<RuleTemplate> getRuleTemplateById(int id){
        return Optional.ofNullable(ruleTemplateRepo.findById(id).orElse(null));
    }

    public RuleTemplate getRuleTemplateByName(String Name){
        return (RuleTemplate) ruleTemplateRepo.findByRuletemplateName(Name).orElse(null);
    }

    public RuleTemplate saveAllRuleTemplate(List<RuleTemplate> ruleTemplates){
        return (RuleTemplate) ruleTemplateRepo.saveAll(ruleTemplates);
    }

    public RuleTemplate saveRuleTemplate(RuleTemplate ruleTemplate){
        return ruleTemplateRepo.save(ruleTemplate);
    }

    public String deleteByRuleTemplateId(int id){
        ruleTemplateRepo.deleteById(id);
        return "deleted Rule Template Id" + id;
    }

}
