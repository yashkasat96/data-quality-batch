import { useEffect, useState } from 'react';
import {
  Table,
  Button,
  Modal,
  Space,
  Tag,
  Form,
  Row,
  Input,
  Typography,
  message,
  Popconfirm,
} from 'antd';
import styles from './ruleset.module.scss';
import RulesetForm from '../../components/RulesetForm/RulesetForm';
import { v4 as uuidv4 } from 'uuid';
import { RulesetFormData } from '../../interfaces/ruleset';

const { Text } = Typography;

const RuleSet = () => {
  const [ruleset, setRuleset] = useState<Array<RulesetFormData>>([]);
  const [currentRuleset, setCurrentRuleSet] = useState<RulesetFormData | null>(null);
  const [rulesetForm] = Form.useForm();
  const [searchText, setSearchText] = useState('');
  const [filteredData, setFilteredData] = useState<Array<RulesetFormData>>([]);

  const handleSearch = (text: string) => {
    setSearchText(text);
    const filteredRules = ruleset.filter((ruleset: RulesetFormData) =>
      ruleset.ruleSetName.toLowerCase().includes(text.toLowerCase())
    );
    setFilteredData(filteredRules);
  };

  const createRuleset = (submitType: 'submit' | 'draft') => {
    rulesetForm
      .validateFields()
      .then(() => {
        const savedData = localStorage.getItem('ruleset');
        const values = rulesetForm.getFieldsValue();

        let allData = [];
        if (savedData) {
          allData = JSON.parse(savedData);
        }
        allData.push({ ...values, id: uuidv4() });
        localStorage.setItem('ruleset', JSON.stringify(allData));
        setIsModalVisible(false);
        setRuleset(allData);
        rulesetForm.resetFields();
        message.success(
          values.isDraft ? 'Ruleset Draft created successfully !' : 'Ruleset created successfully !'
        );
      })
      .catch(errorInfo => {
        // Form has validation errors
      });
  };

  const editRuleSet = ({ record = undefined }: any) => {
    rulesetForm
      .validateFields()
      .then(() => {
        const values = record ? record : rulesetForm.getFieldsValue();

        const currentRuleSetId = record ? record.id : currentRuleset?.id;
        const savedData = localStorage.getItem('ruleset');

        let allData = [];
        if (savedData) {
          allData = JSON.parse(savedData).map((ruleset: RulesetFormData) => {
            if (ruleset.id === currentRuleSetId) {
              return { ...ruleset, ...values };
            }
            return ruleset;
          });
        }
        localStorage.setItem('ruleset', JSON.stringify(allData));
        setIsModalVisible(false);
        setRuleset(allData);
        rulesetForm.resetFields();

        message.success(
          record ? 'Ruleset draft submitted successfully !' : 'Ruleset edited successfully !'
        );
      })
      .catch(errorInfo => {
        // Form has validation errors
      });
  };

  const deleteRuleset = (ruleset: RulesetFormData) => {
    let rulesets = localStorage.getItem('ruleset');
    const parsedRuleSets = JSON.parse(rulesets || '') || [];
    if (parsedRuleSets) {
      let filteredRules = parsedRuleSets.filter((rs: RulesetFormData) => rs.id !== ruleset.id);
      localStorage.setItem('rule', JSON.stringify(filteredRules));
      setRuleset(filteredRules);
      message.success('Ruleset deleted successfully!');
    }
  };

  const columns = [
    {
      title: 'Ruleset Name',
      dataIndex: 'ruleSetName',
      key: 'ruleSetName',
      render: (name: string, record: RulesetFormData) => (
        <>
          <Text>{name}</Text> {record?.isDraft ? <Tag>Draft</Tag> : null}
        </>
      ),
    },
    {
      title: 'Ruleset Description',
      dataIndex: 'rulesetDescription',
      key: 'rulesetDescription',
    },
    {
      title: 'Notification Email',
      dataIndex: 'notificationEmail',
      key: 'notificationEmail',
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: RulesetFormData) => (
        <Space size='middle'>
          <a
            onClick={() => {
              setCurrentRuleSet(record);
              showModal();
            }}
          >
            Edit
          </a>
          <Popconfirm
            title='Delete the Ruleset'
            description='Are you sure to delete this ruleset?'
            onConfirm={() => {
              deleteRuleset(record);
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
                editRuleSet({ record });
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
    const savedData = localStorage.getItem('ruleset');
    if (savedData) {
      setRuleset(JSON.parse(savedData));
    }
  }, []);

  useEffect(() => {
    setFilteredData(ruleset);
  }, [ruleset]);

  const [isModalVisible, setIsModalVisible] = useState(false);

  const showModal = () => {
    setIsModalVisible(true);
  };

  const handleCancel = () => {
    setIsModalVisible(false);
  };

  return (
    <div>
      <Row style={{ float: 'right' }}>
        <Button
          type='primary'
          onClick={() => {
            setCurrentRuleSet(null);
            showModal();
          }}
          className={styles.createButton}
        >
          Create Ruleset
        </Button>
      </Row>
      <div>
        <Input
          placeholder='Search Ruleset Name'
          value={searchText}
          onChange={e => handleSearch(e.target.value)}
          style={{ width: 300 }}
        />
        <Table columns={columns} dataSource={filteredData} />
      </div>

      <Modal
        title='Create Ruleset'
        open={isModalVisible}
        onCancel={handleCancel}
        width={'70%'}
        key={uuidv4()}
        footer={null}
      >
        <RulesetForm
          editRuleSet={editRuleSet}
          rulesetForm={rulesetForm}
          createRuleset={createRuleset}
          currentRuleset={currentRuleset}
        />
      </Modal>
    </div>
  );
};

export default RuleSet;
