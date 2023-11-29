import { Modal, Table, Row, Col } from 'antd';
import { formatString } from '../../common/utilities/utils';

type RuleDetailsModalProps = {
  ruleData: any;
  isModalVisible: boolean;
  handleModalClose: () => void;
};
const RuleDetailsModal = ({
  ruleData,
  isModalVisible,
  handleModalClose,
}: RuleDetailsModalProps) => {
  const columns = [
    {
      title: 'Property Name',
      dataIndex: 'propertyName',
      key: 'propertyName',
    },
    {
      title: 'Property Value',
      dataIndex: 'propertyValue',
      key: 'propertyValue',
    },
  ];

  return (
    <Modal
      title='Rule Details'
      open={isModalVisible}
      onOk={handleModalClose}
      onCancel={handleModalClose}
      width={800} // Set the width for the modal
      centered // Center the modal on the screen
      footer={null} // Remove the footer
    >
      <div style={{ marginBottom: 20 }}>

        <Row gutter={24}>
          <Col span={24}>
            <p>
              <strong>Rule Name:</strong> {ruleData?.ruleName}
            </p>
          </Col>
        </Row>
        <Row gutter={24}>
          <Col span={24}>
            <strong>Rule Description</strong>
            <p>{ruleData?.ruleDescription}</p>
          </Col>
        </Row>
        <Row gutter={24}>
          <Col span={12}>
            <p>
              <strong>Rule Template Name:</strong> {formatString(ruleData?.ruleTemplateName)}
            </p>
          </Col>
          <Col span={12}>
            <p>
              <strong>DQ Metric:</strong> {formatString(ruleData?.dqMetric)}
            </p>
          </Col>
        </Row>
        <Row gutter={24}>
          <Col span={12}>
            <p>
              <strong>Rule Set Name:</strong> {ruleData?.ruleSetName}
            </p>
          </Col>
          <Col span={12}>
            <p>
              <strong>Primary Source Entity:</strong> {ruleData?.primarySourceEntity}
            </p>
          </Col>
        </Row>
        <Row gutter={24}>
          <Col span={12}>
            <p>
              <strong>Primary Target Entity:</strong> {ruleData?.primaryTargetEntity}
            </p>
          </Col>
          <Col span={12}>
            <p>
              <strong>Secondary Source Entity:</strong> {ruleData?.secondarySourceEntity}
            </p>
          </Col>
        </Row>
        <Row gutter={24}>
          <Col span={12}>
            <p>
              <strong>Secondary Target Entity:</strong> {ruleData?.secondaryTargetEntity}
            </p>
          </Col>
        </Row>
      </div>

      <h3>Properties:</h3>
      <Table dataSource={ruleData?.properties} columns={columns} pagination={false} />
    </Modal>
  );
};

export default RuleDetailsModal;
