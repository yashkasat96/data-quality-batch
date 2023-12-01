import React, { Dispatch, SetStateAction, useEffect, useMemo, useState } from 'react';
import {
  Form,
  Input,
  Button,
  Row,
  Col,
  Select,
  Table,
  FormInstance,
  Modal,
  Space,
  Checkbox,
} from 'antd';
import { RULE_TEMPLATES, RULE_TEMPLATE_PROPERTIES } from '../../data';
import { v4 as uuidv4 } from 'uuid';
import { PlusOutlined, MinusCircleOutlined } from '@ant-design/icons';
import { CreateRule } from '../../pages/rule/rule';
import { RuleFormData } from '../../interfaces/rule';
import { IEntity } from '../../interfaces';
import { formatString } from '../../common/utilities/utils';

const { Option } = Select;
type RulesetFormProps = {
  currentRule?: RuleFormData | null;
  isModalVisible: boolean;
  handleCancel: () => void;
  editRule: ({ record }: { record?: RuleFormData }) => void;
  ruleForm: FormInstance<RuleFormData>;
  createRule: ({ submitType, record }: CreateRule) => void;
};

const RuleFormModal = (props: RulesetFormProps) => {
  const { currentRule, isModalVisible, handleCancel, editRule, ruleForm, createRule } = props;
  const [availableDQMetics, setAvailableDQMetics] = useState<string[]>([]);
  const [entities, setEntity] = useState<Array<IEntity>>([]);
  const [ruleset, setRuleset] = useState<any>([]);
  const [secondaryEntities, setSecondaryEntities] = useState<Array<IEntity>>([]);

  useEffect(() => {
    if (currentRule) {
      ruleForm.setFieldsValue(currentRule);
    }
    const entity = localStorage.getItem('entities');
    if (entity) {
      setEntity(JSON.parse(entity));
    }

    const ruleset = localStorage.getItem('ruleset');
    if (ruleset) {
      setRuleset(JSON.parse(ruleset));
    }

    return () => {
      ruleForm.resetFields();
    };
  }, []);

  const handleRuleTemplateChange = (value: string) => {
    // Filter available DQ Metrics based on the selected Rule Template
    const metrics = RULE_TEMPLATES.filter(template => template.rule_template_name === value).map(
      template => template.dq_metric
    );

    setAvailableDQMetics(metrics);
    ruleForm.setFieldsValue({ dqMetric: undefined }); // Reset the DQ Metric
  };

  const handlePrimarySourceChange = (value: string) => {
    // Filter entities for secondary source and target
    const secondaryEntitiesFiltered = entities.filter(
      (entity: IEntity) => entity.entityName !== value
    );
    setSecondaryEntities(secondaryEntitiesFiltered);

    // Clear secondary source and target selections
    ruleForm.setFieldsValue({ secondarySourceEntity: undefined, secondaryTargetEntity: undefined });
  };

  const handleDQMetricChange = (value: string) => {
    const ruleTemplateId = RULE_TEMPLATES.filter(
      ruleTemplates =>
        ruleTemplates.rule_template_name === ruleForm.getFieldValue('ruleTemplateName') &&
        ruleTemplates.dq_metric.toLowerCase() === value.toLowerCase()
    )[0].rule_template_id;

    const filteredProps = RULE_TEMPLATE_PROPERTIES.filter(
      prop =>
        prop.rule_template_id === ruleTemplateId && prop.rule_template_prop_type !== 'PREDEFINED'
    );

    // Set the new properties on the form
    const newProperties = filteredProps.map(prop => ({
      propertyName: prop.rule_template_prop_key,
      propertyValue: '',
      propertyType: prop.rule_template_prop_type,
      isMandatory: prop.is_mandatory,
    }));

    ruleForm.setFieldsValue({ properties: newProperties });
  };

  const enableTargetEntities = useMemo(
    () => ruleForm.getFieldValue('ruleTemplateName') === 'DATA_DIFF',
    [ruleForm.getFieldValue('ruleTemplateName')]
  );
  return (
    <Modal
      title='Create Rule'
      open={isModalVisible}
      onCancel={handleCancel}
      width={'70%'}
      footer={null}
    >
      <Form
        layout='vertical'
        form={ruleForm}
        name='ruleForm'
        // onFinish={currentRule ? editRule : createRule}
      >
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label='Rule Template Name'
              name='ruleTemplateName'
              rules={[
                {
                  required: true,
                  message: 'Rule Template is required',
                },
              ]}
            >
              <Select placeholder='Select Rule Template Name' onChange={handleRuleTemplateChange}>
                {RULE_TEMPLATES.filter(ruleTemplate => ruleTemplate).map(template => (
                  <Option key={template.rule_template_id} value={template.rule_template_name}>
                    {formatString(template.rule_template_name)}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label='DQ Metric'
              name='dqMetric'
              rules={[
                {
                  required: true,
                  message: 'DQ Metric is required',
                },
              ]}
            >
              <Select placeholder='Select DQ Metric' onChange={handleDQMetricChange}>
                {availableDQMetics.map(metric => (
                  <Option key={metric} value={formatString(metric)}>
                    {metric}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
        </Row>
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              name='ruleName'
              label='Rule Name'
              rules={[
                {
                  required: true,
                  message: 'Rule Name is required',
                },
              ]}
            >
              <Input />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              name='ruleDescription'
              label='Rule Description'
              rules={[
                {
                  required: true,
                  message: 'Rule Description is required',
                },
              ]}
            >
              <Input />
            </Form.Item>
          </Col>
        </Row>
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item label='Ruleset Name' name='ruleSetName'>
              <Select placeholder='Select RuleSet Name'>
                {ruleset.map((ruleset: any) => (
                  <Option key={ruleset.ruleSetName} value={ruleset.ruleSetName}>
                    {ruleset.ruleSetName}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
          <Col span={12}></Col>
        </Row>

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label='Primary Source Entity'
              name='primarySourceEntity'
              rules={[{ required: true, message: 'Primary Source Entity is required' }]}
            >
              <Select
                placeholder='Select Primary Source Entity'
                onChange={handlePrimarySourceChange}
              >
                {entities.map((entity: IEntity, index: any) => (
                  <Option key={index} value={entity.entityName}>
                    {entity.entityName}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label='Primary Target Entity'
              name='primaryTargetEntity'
              rules={[
                {
                  required: enableTargetEntities,
                  message: 'Primary Source Entity is required',
                },
              ]}
            >
              <Select placeholder='Select Primary Target Entity' disabled={!enableTargetEntities}>
                {entities.map((entity: IEntity, index: any) => (
                  <Option key={index} value={entity.entityName}>
                    {entity.entityName}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item label='Secondary Source Entity' name='secondarySourceEntity'>
              <Select placeholder='Select Secondary Source Entity'>
                {secondaryEntities.map((entity, index) => (
                  <Option key={index} value={entity.entityName}>
                    {entity.entityName}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item label='Secondary Target Entity' name='secondaryTargetEntity'>
              <Select placeholder='Select Secondary Target Entity' disabled={!enableTargetEntities}>
                {secondaryEntities.map((entity, index) => (
                  <Option key={index} value={entity.entityName}>
                    {entity.entityName}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
        </Row>
        <h2>Properties:</h2>

        <Form.List name='properties'>
          {(fields, { add, remove }) => (
            <>
              {fields.map(({ key, name, ...restField }, index) => {
                // Assume 'isMandatory' is a boolean that indicates if the property is mandatory
                const isMandatory = ruleForm.getFieldValue(['properties', name, 'isMandatory']);
                return (
                  <Row key={key} gutter={16} justify={'center'}>
                    <Col span={12}>
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
                        <Input />
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
                        <Input />
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
              <Button
                type='primary'
                htmlType='submit'
                style={{ float: 'right' }}
                onClick={
                  currentRule
                    ? () => editRule({ record: undefined })
                    : () => createRule({ submitType: 'submit' })
                }
              >
                {currentRule ? 'Update' : 'Submit'}
              </Button>
            </Form.Item>
          </Space>
        </Row>
      </Form>
    </Modal>
  );
};

export default RuleFormModal;
