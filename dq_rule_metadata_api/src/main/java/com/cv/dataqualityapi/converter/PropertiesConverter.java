package com.cv.dataqualityapi.converter;

import java.util.Properties;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Converter
public class PropertiesConverter implements AttributeConverter<Properties, String>{

	private static final ObjectMapper mapper = new ObjectMapper();
	
	@Override
	public String convertToDatabaseColumn(Properties attribute) {
		try {
			return mapper.writeValueAsString(attribute);
		} catch (JsonProcessingException e) {
		    throw new IllegalArgumentException(e.getMessage());
		}
	}

	@Override
	public Properties convertToEntityAttribute(String dbData) {
		try {
			return mapper.readValue(dbData, Properties.class);
		} catch (JsonProcessingException e) {			
			throw new IllegalArgumentException(e.getMessage());
		}
	}

}
