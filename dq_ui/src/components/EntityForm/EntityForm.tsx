import React, { Dispatch, SetStateAction, useEffect } from 'react';
import { Form, Input, Select, Button, Row, Col, message, Checkbox, Space, Modal } from 'antd';
import { PlusOutlined, MinusCircleOutlined } from '@ant-design/icons';
import { ENTITY_TEMPLATES, ENTITY_TEMPLATE_PROPERTIES } from '../../data';
import { IEntityTemplate, IGroupedOption } from '../../interfaces';
import { formatString } from '../../common/utilities/utils';

const { Option, OptGroup } = Select;

type EntityFormProps = {
  setIsModalVisible: Dispatch<SetStateAction<boolean>>;
  setData: Dispatch<SetStateAction<any>>;
  entityToEdit?: any; // You should type this according to the shape of the entity you expect
};

const EntityForm = (props: EntityFormProps) => {
  const { setIsModalVisible, setData, entityToEdit } = props;
  const [form] = Form.useForm();

  useEffect(() => {
    // Check if there is an entity to edit and set the form fields
    console.log(entityToEdit);
    if (entityToEdit) {
      form.setFieldsValue(entityToEdit);
    }
    return () => {
      form.resetFields();
    };
  }, [entityToEdit, form]);

  const handleSubmit = (values: any) => {
    console.log(values);
    let allData = JSON.parse(localStorage.getItem('entities') || '[]');

    if (entityToEdit) {
      // Find the index of the entity to edit and update it
      const index = allData.findIndex((entity: any) => entity.id === entityToEdit.id);
      allData[index] = { ...entityToEdit, ...values };
      message.success('Entity updated successfully!');
    } else {
      // If we're creating a new entity, add it to the array
      allData.push({ id: Date.now(), ...values });
      message.success('Entity created successfully!');
    }

    localStorage.setItem('entities', JSON.stringify(allData));
    setIsModalVisible(false);
    setData(allData);
    form.resetFields();
  };

  const handleEntitySubTypeChange = (value: string) => {
    // Clear existing properties
    form.setFieldsValue({ properties: [] });

    // Find the corresponding entity_type for the selected entity_subtype
    const entityType = ENTITY_TEMPLATES.find(template => template.entity_subtype === value)
      ?.entity_type;

    // If an entity_type is found, set it in the form
    if (entityType) {
      form.setFieldsValue({ entity_type: entityType });

      // Filter the JSON data for properties based on the selected subtype
      const filteredProps = ENTITY_TEMPLATE_PROPERTIES.filter(
        prop => prop.entity_type === entityType
      );

      // Set the new properties on the form
      const newProperties = filteredProps.map(prop => ({
        propertyName: prop.entity_template_prop_key,
        propertyValue: '', // Leave value empty
        propertyType: prop.entity_template_prop_type,
        isMandatory: prop.is_mandatory,
      }));

      form.setFieldsValue({ properties: newProperties });
    }
  };

  const generateGroupedOptions = (entityTemplates: IEntityTemplate[]) => {
    const groupedOptions: Record<string, IGroupedOption[]> = {};

    entityTemplates.forEach(template => {
      const { entity_type, entity_subtype } = template;
      if (!groupedOptions[entity_type]) {
        groupedOptions[entity_type] = [];
      }
      groupedOptions[entity_type].push({
        value: entity_subtype,
        label: formatString(entity_subtype),
      });
    });
    return Object.entries(groupedOptions).map(([groupLabel, options]) => (
      <OptGroup key={groupLabel} label={groupLabel}>
        {options.map(option => (
          <Option key={option.value} value={option.value}>
            {option.label}
          </Option>
        ))}
      </OptGroup>
    ));
  };

  return (
    <Form layout='vertical' form={form} name='entityForm' onFinish={handleSubmit}>
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            name='entity_subtype'
            label='Entity SubType'
            rules={[
              {
                required: true,
                message: 'Entity SubType is required',
              },
            ]}
          >
            <Select onChange={value => handleEntitySubTypeChange(value)}>
              {generateGroupedOptions(ENTITY_TEMPLATES)}
            </Select>
          </Form.Item>
          <Form.Item name='entity_type' hidden />
        </Col>
      </Row>
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            name='entityName'
            label='Entity Name'
            rules={[
              {
                required: true,
                message: 'Entity Name is required',
              },
            ]}
          >
            <Input autoComplete='off' />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            name='entityPhysicalName'
            label='Entity Physical Name'
            rules={[
              {
                required: true,
                message: 'Entity Physical Name is required',
              },
            ]}
          >
            <Input autoComplete='off' />
          </Form.Item>
        </Col>
      </Row>
      <h3>Properties</h3>
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            name='primaryKey'
            label='Primary Key'
            rules={[
              {
                required: true,
                message: 'Primary Key is required',
              },
            ]}
          >
            <Input autoComplete='off' />
          </Form.Item>
        </Col>
      </Row>
      <Form.List name='properties'>
        {(fields, { add, remove }) => (
          <>
            {fields.map(({ key, name, ...restField }, index) => {
              // Assume 'isMandatory' is a boolean that indicates if the property is mandatory
              const isMandatory = form.getFieldValue(['properties', name, 'isMandatory']);
              return (
                <Row key={key} gutter={16} justify={'center'}>
                  <Col span={8}>
                    <Form.Item
                      {...restField}
                      name={[name, 'propertyName']}
                      label={`Property ${index + 1}`}
                      rules={[
                        {
                          required: isMandatory,
                          message: 'Property Name is required',
                        },
                      ]}
                    >
                      <Input autoComplete='off' />
                    </Form.Item>
                  </Col>
                  <Col span={4}>
                    <Form.Item {...restField} name={[name, 'propertyType']} label='Property Type'>
                      <Select>{<option value={'VARIABLE'} label='Variable'></option>}</Select>
                    </Form.Item>
                  </Col>
                  <Col span={10}>
                    <Form.Item
                      {...restField}
                      name={[name, 'propertyValue']}
                      rules={[
                        {
                          required: isMandatory,
                          message: 'Property Name is required',
                        },
                      ]}
                      label='Property Value'
                    >
                      <Input autoComplete='off' />
                    </Form.Item>
                  </Col>
                  {/* Conditionally render the delete button */}
                  <Col span={2}>
                    {!isMandatory && (
                      <Form.Item label=' '>
                        <Button icon={<MinusCircleOutlined />} onClick={() => remove(name)} />
                      </Form.Item>
                    )}
                  </Col>
                </Row>
              );
            })}
            <Form.Item>
              <Button type='dashed' onClick={() => add()} icon={<PlusOutlined />}>
                Add Property
              </Button>
            </Form.Item>
          </>
        )}
      </Form.List>
      <Row justify={'end'}>
        <Space align='center'>
          <Form.Item name='isDraft' valuePropName='checked'>
            <Checkbox>Save as Draft</Checkbox>
          </Form.Item>
          <Form.Item>
            <Button type='primary' htmlType='submit' style={{ float: 'right' }}>
              {entityToEdit ? 'Update' : 'Submit'}
            </Button>
          </Form.Item>
        </Space>
      </Row>
    </Form>
  );
};

export default EntityForm;
