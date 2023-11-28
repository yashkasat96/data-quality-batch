package com.cv.dataqualityapi.dto.CreateDto;
import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateEntityTemplateDto {

    private Integer entitytemplateId;

    private String entitytemplateType;

    private String entitytemplateSubtype;

    private String entitytemplateCreatedBy;

    private String entitytemplateUpdatedBy;

    private Date entitytemplateCreatedDate;

    private Date entitytemplateUpdatedDate;

}