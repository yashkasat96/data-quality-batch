import React, { useEffect } from 'react';
import { Form, Input, Button, Row, Col, FormInstance, Space, Checkbox } from 'antd';
import { RulesetFormData } from '../../interfaces/ruleset';

type RulesetFormProps = {
  currentRuleset: RulesetFormData | null;
  rulesetForm: FormInstance<RulesetFormData>;
  editRuleSet: ({ record }: any) => void;
  createRuleset: (submitType: 'submit' | 'draft') => void;
};
const RulesetForm = (props: RulesetFormProps) => {
  const { currentRuleset, rulesetForm, createRuleset, editRuleSet } = props;

  useEffect(() => {
    if (currentRuleset) {
      rulesetForm.setFieldsValue(currentRuleset);
    }
  }, []);

  return (
    <Form layout='vertical' form={rulesetForm} name='rulesetForm'>
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            name='ruleSetName'
            label='Ruleset Name'
            rules={[
              {
                required: true,
                message: 'Ruleset Name is required',
              },
            ]}
          >
            <Input />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            name='rulesetDescription'
            label='Ruleset Description'
            rules={[
              {
                required: true,
                message: 'Ruleset Description is required',
              },
            ]}
          >
            <Input />
          </Form.Item>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            name='notificationEmail'
            label='Notification Email'
            rules={[
              {
                required: true,
                message: 'Notification Email is required',
              },
              {
                type: 'email',
                message: 'Please enter a valid email address',
              },
            ]}
          >
            <Input />
          </Form.Item>
        </Col>
        <Col span={12}></Col>
      </Row>

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
              onClick={currentRuleset ? editRuleSet : () => createRuleset('submit')}
            >
              {currentRuleset ? 'Update' : 'Submit'}
            </Button>
          </Form.Item>
        </Space>
      </Row>
      {/* <Row gutter={16}>
        <Col span={12}></Col>
        <Col span={12}>
          <Col span={12} style={{ float: 'right' }}>
            {!currentRuleset && (
              <Form.Item>
                <Button type='primary' htmlType='submit' onClick={() => createRuleset('draft')}>
                  Save as Draft
                </Button>
              </Form.Item>
            )}
          </Col>
          <Col span={12} style={{ float: 'right' }}>
            <Form.Item>
              <Button
                type='primary'
                htmlType='submit'
                onClick={currentRuleset ? editRuleSet : () => createRuleset('submit')}
              >
                {currentRuleset ? 'Edit ruleset' : 'Save & Submit Ruleset'}
              </Button>
            </Form.Item>
          </Col>
        </Col>
      </Row> */}
    </Form>
  );
};

export default RulesetForm;
