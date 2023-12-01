package com.cv.dataqualityapi.rest.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.bigquery.core.BigQueryTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class BigQueryController {

	@Autowired
	private BigQuery bigQuery;

	@GetMapping
	public void getData() throws JobException, InterruptedException {
		String query = "select * from entity_template_properties_copy";

		QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(query).build();
		TableResult result = bigQuery.query(queryJobConfiguration);

		log.info("the bigquery result is: {}", result.getValues().iterator().next());

	}
}
