package com.cv.dataqualityapi.model;

import java.util.Date;
import java.util.Set;

import javax.persistence.*;

import com.fasterxml.jackson.annotation.JsonBackReference;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor

@Table(name = "entity_template")
public class EntityTemplate {

    @Id
  //  @GeneratedValue(strategy = GenerationType.IDENTITY)
    @ApiModelProperty(notes = "EntityDetails Id", example = "1", required = true)
    @Column(name = "entity_template_id")
    private Integer entitytemplateId;

    @ApiModelProperty(notes = "entity_type", example = "Source")
    @Column(name = "entity_type")
    private String entityType;

    @ApiModelProperty(notes = "sub type of entity", example = "FILE")
    @Column(name = "entity_subtype")
    private String entitySubtype;

    @ApiModelProperty(notes = "created by name", example = "Clairvoyant")
    @Column(name = "created_by")
    private String entitytemplateCreatedBy;

    @ApiModelProperty(notes = "updated by name", example = "Clairvoyant")
    @Column(name = "updated_by")
    private String entitytemplateUpdatedBy;

    @Column(name = "created_date")
    @ApiModelProperty(notes = "Entity template creation timestamp", example = "2023-04-18 14:20:20.785", required = false)
    @Temporal(TemporalType.DATE)
    private Date entitytemplateCreatedDate;

    @Column(name = "updated_date")
    @ApiModelProperty(notes = "Entity updation timestamp", example = "2023-04-18 14:20:20.785", required = false)
    @Temporal(TemporalType.DATE)
    private Date entitytemplateUpdatedDate;

    @OneToMany(mappedBy = "entityTemp", cascade = CascadeType.ALL,fetch = FetchType.LAZY)
    private Set<EntityTemplateProperties> entityTemProp;

    @OneToMany(mappedBy = "entityTemp", cascade = CascadeType.ALL,fetch = FetchType.LAZY)
    private Set<Entities> entity;

}
