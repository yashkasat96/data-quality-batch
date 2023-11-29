export interface IEntityTemplate {
    entity_template_id: number;
    entity_type: string;
    entity_subtype: string;
}

export interface IGroupedOption {
    value: string;
    label: string;
}

export interface IEntityProperty {
    propertyName: string;
    propertyValue: string;
    propertyType: string;
    isMandatory: boolean;
  }
  
  export interface IEntity {
    isDraft: any;
    entity_subtype: string;
    entity_type: string;
    entityName: string;
    entityPhysicalName: string;
    primaryKey: string;
    properties: IEntityProperty[];
  }
  
  export interface IEntityDetailsModalProps {
    entity: IEntity;
  }