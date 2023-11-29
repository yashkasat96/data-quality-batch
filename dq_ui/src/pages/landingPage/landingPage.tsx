import React from 'react';
import { Layout, Typography, Row, Col, Card  } from 'antd';
import styles from './landingPage.module.scss';
import logo from '../../assets/exl_logo_rgb_orange_pos.png';
import RULE_IMG from "../../assets/Analyze_documents.png";
import ENTITIES_IMG from "../../assets/Data_management.png";
import REPORTS_IMG from "../../assets/Quality.png";
import { DatabaseOutlined, CheckCircleOutlined, ClockCircleOutlined } from '@ant-design/icons';

const { Header, Content } = Layout;
const { Title } = Typography;

const LandingPage = () => {
  return (
    <Layout style={{ height: 'calc(100vh - 32px)'}}>
      <Content className={styles.centerContent}>
        <img src={logo} alt="EXL Analytics" className={styles.logo} />
        <Title style={{
          fontSize: '5rem',
          margin: 0,
          color: '#292d34',
          fontWeight: 300,

        }} className={styles.productName}>
          <span></span>Data Quality</Title>
        <Title style={{
          fontWeight: 400,
          fontSize: '1.15rem'
        }}
        level={4} className={styles.productSubtitle}>Seamless data quality management with pre-built rule templates for every data source.</Title>
        <Row gutter={[16, 16]} className={styles.productDetails}>
          <Col span={8}>
            <Card className={styles.featureCard}>
              <img src={ENTITIES_IMG} alt="Entities" className={styles.logo} />
              {/* <Title level={4}>Entities</Title> */}
              <ul>
                <li>Comprehensive data structure analysis</li>
                <li>Automated entity recognition</li>
                <li>Support for multiple entity types</li>
              </ul>
            </Card>
          </Col>
          <Col span={8}>
            <Card className={styles.featureCard}>
            <img src={RULE_IMG} alt="Entities" className={styles.logo} />
              {/* <Title level={4}>Rules</Title> */}
              <ul>
                <li>Versatile pre-defined rule templates</li>
                <li>Custom SQL scripting capabilities</li>
                <li>Advanced property mapping</li>
                <li>Flawless execution across databases</li>
              </ul>
            </Card>
          </Col>
          <Col span={8}>
            <Card className={styles.featureCard}>
            <img src={REPORTS_IMG} alt="Entities" className={styles.logo} />
              {/* <Title level={4}>Executions</Title> */}
              <ul>
                <li>Real-time data validation reports</li>
                <li>Scheduled report generation</li>
                <li>Instant alerts and notifications</li>
              </ul>
            </Card>
          </Col>
        </Row>
      </Content>
    </Layout>
  );
};

export default LandingPage;
