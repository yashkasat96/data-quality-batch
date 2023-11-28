package com.cv.dataqualityapi.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.cv.dataqualityapi.Repo.*;
import com.cv.dataqualityapi.dto.*;
import com.cv.dataqualityapi.model.Entities;
import com.cv.dataqualityapi.model.RuleEntityMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cv.dataqualityapi.exception.ResourceNotFoundException;
import com.cv.dataqualityapi.model.RuleSet;
import com.cv.dataqualityapi.model.Rules;
import com.cv.dataqualityapi.service.GenerateRulesJsonService;

@Service
public class GenerateRulesJsonServiceImpl implements GenerateRulesJsonService {

	@Autowired
	private RuleSetRepo ruleSetRepository;

	@Autowired
	private RulesRepo ruleRepository;

	@Autowired
	private EntityPropertiesRepo entityPropertiesRepository;

	@Autowired
	private EntityRepo entityRepository;

	@Autowired
	private EntityTemplatePropertiesRepo entityTemplatePropertiesRepository;

	@Autowired
	private EntityTemplateRepo entityTemplateRepository;

	@Autowired
	private RuleEntityMapRepo ruleEntityMapRepository;

	@Autowired
	private RulePropertiesRepo rulePropertiesRepository;

	@Autowired
	private RuleTemplatePropertiesRepo ruleTemplatePropertiesRepository;

	@Autowired
	private RuleTemplateRepo ruleTemplateRepository;

	@Override
	public JsonResponseDto generateRulesJson(String rulesetName) {

		Optional<RuleSet> ruleSetOp = ruleSetRepository.findByRulesetName(rulesetName);
		if (! ruleSetOp.isPresent()) {
			throw new ResourceNotFoundException("rule set not found");
		}

		JsonResponseDto jsonResponse = new JsonResponseDto();
		List<RulesJsonDto> rulesJsonDtolist = new ArrayList<>();

		RuleSet ruleset = ruleSetOp.get();
		AtomicInteger seq = new AtomicInteger(0);

		ruleset.getRules().stream().forEach(rule -> {

			List<DataEntityAssociations> dataEntityAssociationsList = new ArrayList<>();
			rule.getRuleEntityMap().stream().forEach(ruleMap -> {

				List<EntityPropertiesDto> entityPropList = new ArrayList<>();
				ruleMap.getEntities().getEntityProp().stream().forEach(entityPro ->
				{
					EntityPropertiesDto entityPropListDto = new EntityPropertiesDto();
					entityPropListDto.setKey(entityPro.getEntitypropKey());
					entityPropListDto.setValue(entityPro.getEntitypropValue());
					entityPropList.add(entityPropListDto);
				});

				List<EntityTemplatePropertiesDto> entityTemplatePropList = new ArrayList<>();
				ruleMap.getEntities().getEntityTemp().getEntityTemProp().stream().forEach(entityTemplateProp ->
				{
					EntityTemplatePropertiesDto entityTemplatePropListDto = new EntityTemplatePropertiesDto();
					entityTemplatePropListDto.setEntityTemplatePropDesc(entityTemplateProp.getEntitytemplatepropDesc());
					entityTemplatePropListDto.setEntityTemplatePropKey(entityTemplateProp.getEntitytemplatepropKey());
					entityTemplatePropListDto.setIsMandatory(entityTemplateProp.getIsMandatory());

					entityTemplatePropList.add(entityTemplatePropListDto);
				});

				DataEntityAssociations entityAssociationsDto = new DataEntityAssociations();
				entityAssociationsDto.setEntity_id(ruleMap.getEntities().getEntityId());
				entityAssociationsDto.setEntity_type(ruleMap.getEntities().getEntityTemp().getEntityType());
				entityAssociationsDto.setEntity_behaviour(ruleMap.getRuleEntityMapEntityBehaviour());
				entityAssociationsDto.setEntity_sub_type(ruleMap.getEntities().getEntityTemp().getEntitySubtype());
				entityAssociationsDto.setEntity_name(ruleMap.getEntities().getEntityName());
				entityAssociationsDto.setEntity_physical_name(ruleMap.getEntities().getEntityPhysicalName());
				entityAssociationsDto.setPrimary_key(ruleMap.getEntities().getEntityPrimaryKey());
				entityAssociationsDto.setIs_primary("TRUE");
				entityAssociationsDto.setProperties(entityPropList);
				entityAssociationsDto.setAll_entity_properties(entityTemplatePropList);

				dataEntityAssociationsList.add(entityAssociationsDto);
			});

			List<PropertiesDto> propertyDtoList = new ArrayList<>();
			rule.getRulesprop().stream().forEach(prop -> {
				PropertiesDto propDto = new PropertiesDto();
				propDto.setKey(prop.getRulepropertiesKey());
				propDto.setValue(prop.getRulepropertiesValue());
				propDto.setType("VARIABLE");
				propertyDtoList.add(propDto);
			});


			List<RuleTemplateDetailsDTO> ruleTemplateDetailsDtolist = new ArrayList<>();

			List<RuleTemplatePropertiesDTO> ruleTempPropDtoList = new ArrayList<>();
			rule.getRuleTemp().getRuleTempProp().stream().forEach(temp -> {
				RuleTemplatePropertiesDTO ruleTempPropDto = new RuleTemplatePropertiesDTO();
				ruleTempPropDto.setMandatory(temp.getRuletemplatepropertiesIsMandatory());
				ruleTempPropDto.setDescription(temp.getRuletemplatepropertiesDesc());
				ruleTempPropDto.setType(temp.getRuletemplatepropertiesType());
				ruleTempPropDto.setKey(temp.getRuletemplatepropertiesKey());
				ruleTempPropDtoList.add(ruleTempPropDto);
			});
			RuleTemplateDetailsDTO ruleTemplateDetailsDto = new RuleTemplateDetailsDTO();
			ruleTemplateDetailsDto.setId(rule.getRuletemplateId());
			ruleTemplateDetailsDto.setName(rule.getRuleTemp().getRuletemplateName());
			ruleTemplateDetailsDto.setDescription(rule.getRuleTemp().getRuletemplateDesc());
			ruleTemplateDetailsDto.setTemplateProperties(ruleTempPropDtoList);

			RuleDetailsDto ruleDetailsDto = new RuleDetailsDto();
			ruleDetailsDto.setId(rule.getRuleId());
			ruleDetailsDto.setName(rule.getRuleName());
			ruleDetailsDto.setDescription(rule.getRuleDesc());
		    ruleDetailsDto.setDq_metric(rule.getRuleTemp().getRuletemplateDqMetric());
			ruleDetailsDto.setData_entity_associations(dataEntityAssociationsList);
			ruleDetailsDto.setProperties(propertyDtoList);
			ruleDetailsDto.setTemplate(ruleTemplateDetailsDto);

			RulesJsonDto rulesJsonDto = new RulesJsonDto();
			rulesJsonDto.setSequence(seq.incrementAndGet());
			rulesJsonDto.setStatus("ACTIVE");
			rulesJsonDto.setRuleDetails(ruleDetailsDto);
			rulesJsonDtolist.add(rulesJsonDto);

		});

		List<String> notificationPreferences = new ArrayList<>();
		notificationPreferences.add("EMAIL");

		jsonResponse.setRuleset_id(ruleset.getRulesetId());
		jsonResponse.setRuleset_name(ruleset.getRulesetName());
		jsonResponse.setRuleset_desc(ruleset.getRulesetDesc());
		jsonResponse.setNotification_preference(notificationPreferences);
		jsonResponse.setRules(rulesJsonDtolist);

		return jsonResponse;
	}

}