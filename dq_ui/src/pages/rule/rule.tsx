import { useEffect, useState } from 'react';
import { Table, Button, Space, Tag, Input, Row, Form, Typography, message, Popconfirm } from 'antd';
import styles from './rule.module.scss';
import { v4 as uuidv4 } from 'uuid';
import RuleDetailsModal from '../../components/RuleForm/RuleDetailsModal';
import RuleFormModal from '../../components/RuleForm/RuleFormModal';
import { RuleFormData } from '../../interfaces/rule';
import { formatString } from '../../common/utilities/utils';

const { Text } = Typography;

export type CreateRule = {
  submitType: 'submit' | 'draft';
  record?: RuleFormData;
};
const Rule = () => {
  const [rule, setRule] = useState<Array<RuleFormData>>([]);
  const [currentRule, setCurrentRule] = useState<RuleFormData | null>(null);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [isRuleModalVisible, setIsRuleModalVisible] = useState(false);
  const [searchText, setSearchText] = useState('');
  const [filteredData, setFilteredData] = useState(rule);
  const [ruleForm] = Form.useForm<RuleFormData>();

  const handleSearch = (text: string) => {
    setSearchText(text);
    const filteredRules = rule.filter((rule: any) =>
      rule.ruleName.toLowerCase().includes(text.toLowerCase())
    );
    setFilteredData(filteredRules);
  };

  const editRule = ({ record = undefined }: { record?: RuleFormData }) => {
    ruleForm
      .validateFields()
      .then(() => {
        const savedData = localStorage.getItem('rule');
        const values = record ? record : ruleForm.getFieldsValue();
        const currentRuleId = record ? record.id : currentRule?.id;

        let allData = [];
        if (savedData) {
          allData = JSON.parse(savedData).map((rule: RuleFormData) => {
            if (rule.id === currentRuleId) {
              return { ...rule, ...values };
            }
            return rule;
          });
        }
        localStorage.setItem('rule', JSON.stringify(allData));
        setIsModalVisible(false);
        setRule(allData);
        ruleForm.resetFields();
        message.success(
          record ? 'Rule Draft submitted successfully!' : 'Rules Edited successfully'
        );
      })
      .catch(errorInfo => {
        // Form has validation errors
      });
  };

  const createRule = ({ submitType, record = undefined }: CreateRule) => {
    ruleForm
      .validateFields()
      .then(() => {
        const savedData = localStorage.getItem('rule');
        const values = record ? record : ruleForm.getFieldsValue();
        let allData = [];
        if (savedData) {
          allData = JSON.parse(savedData);
        }

        allData.push({ ...values, id: uuidv4() });
        localStorage.setItem('rule', JSON.stringify(allData));
        setIsModalVisible(false);
        setRule(allData);
        ruleForm.resetFields();

        message.success('Rules created successfully!');
      })
      .catch(errorInfo => {
        // Form has validation errors
      });
  };

  const deleteRule = (rule: RuleFormData) => {
    let rules = localStorage.getItem('rule');
    const parsedRules = JSON.parse(rules || '') || [];
    if (parsedRules) {
      let filteredRules = parsedRules.filter((rl: RuleFormData) => rl.id !== rule.id);
      localStorage.setItem('rule', JSON.stringify(filteredRules));
      setRule(filteredRules);
      message.success('Rules deleted successfully!');
    }
  };

  const columns = [
    {
      title: 'Rule Name',
      dataIndex: 'ruleName',
      key: 'ruleName',
      render: (name: string, record: RuleFormData) => (
        <>
          <Text>{name}</Text> {record?.isDraft ? <Tag>Draft</Tag> : null}
        </>
      ),
    },
    {
      title: 'Rule Template Name',
      dataIndex: 'ruleTemplateName',
      key: 'ruleTemplateName',
      render: (name: string) => <>{formatString(name)}</>,
    },
    {
      title: 'DQ Metric',
      dataIndex: 'dqMetric',
      key: 'dqMetric',
      render: (name: string) => <>{formatString(name)}</>,
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_: string, record: RuleFormData) => (
        <Space size='middle'>
          <a
            onClick={() => {
              setCurrentRule(record);
              showRuleModalDetails();
            }}
          >
            View
          </a>
          <a
            onClick={() => {
              setCurrentRule(record);
              showRuleFormModal();
            }}
          >
            Edit
          </a>
          <Popconfirm
            title='Delete the Rule'
            description='Are you sure to delete this rule?'
            onConfirm={() => {
              deleteRule(record);
            }}
            onCancel={() => {}}
            okText='Delete'
            cancelText='Cancel'
          >
            <a>Delete</a>
          </Popconfirm>
          {record.isDraft && (
            <a
              onClick={() => {
                record.isDraft = false;
                editRule({ record });
              }}
            >
              Submit Draft
            </a>
          )}
        </Space>
      ),
    },
  ];

  useEffect(() => {
    // Load saved enttities from localStorage
    const savedData = localStorage.getItem('rule');
    if (savedData) {
      setRule(JSON.parse(savedData));
    }
  }, []);

  useEffect(() => {
    setFilteredData(rule);
  }, [rule]);

  const showRuleFormModal = () => {
    setIsModalVisible(true);
  };

  const handleCancel = () => {
    setIsModalVisible(false);
  };

  const showRuleModalDetails = () => {
    setIsRuleModalVisible(true);
  };

  const handleRuleModalCancel = () => {
    setIsRuleModalVisible(false);
  };

  return (
    <div>
      <Row style={{ float: 'right' }}>
        <Button
          type='primary'
          onClick={() => {
            setCurrentRule(null);
            showRuleFormModal();
          }}
          className={styles.createButton}
        >
          Create Rule
        </Button>
      </Row>
      <div>
        <Input
          placeholder='Search Rule Name'
          value={searchText}
          onChange={e => handleSearch(e.target.value)}
          style={{ width: 300 }}
        />
        <Table columns={columns} dataSource={filteredData} />
      </div>

      <RuleFormModal
        createRule={createRule}
        editRule={editRule}
        ruleForm={ruleForm}
        isModalVisible={isModalVisible}
        handleCancel={handleCancel}
        key={uuidv4()}
        currentRule={currentRule}
      />

      <RuleDetailsModal
        isModalVisible={isRuleModalVisible}
        ruleData={currentRule}
        handleModalClose={handleRuleModalCancel}
      />
    </div>
  );
};

export default Rule;
