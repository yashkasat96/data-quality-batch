package com.cv.dataqualityapi.service.impl;

import com.cv.dataqualityapi.Repo.RuleTemplatePropertiesRepo;
import com.cv.dataqualityapi.model.RuleTemplateProperties;
import com.cv.dataqualityapi.service.RuleTemplatePropertiesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
@Service
public class RuleTemplatePropertiesServiceImpl implements RuleTemplatePropertiesService {

    @Autowired
    private RuleTemplatePropertiesRepo ruleTemplatePropertiesRepo;

    public RuleTemplateProperties getByRuleTemplatePropertiesId(int id){
        return ruleTemplatePropertiesRepo.findById(id).orElse(null);
    }

    public RuleTemplateProperties getAllRuleTemplateProperties(){
        return (RuleTemplateProperties) ruleTemplatePropertiesRepo.findAll();
    }

    public String deleteRuleTemplatePropertiesById(int id){
        ruleTemplatePropertiesRepo.deleteById(id);
        return "rule Template Properties deleted"+ id ;
    }

    public RuleTemplateProperties saveRuleTemplateProperties(RuleTemplateProperties ruleTempProp){
        return ruleTemplatePropertiesRepo.save(ruleTempProp);
    }

    public RuleTemplateProperties saveAllRuleTemplateProperties(List<RuleTemplateProperties> ruleTempPropList){
        return (RuleTemplateProperties) ruleTemplatePropertiesRepo.saveAll(ruleTempPropList);
    }

}
