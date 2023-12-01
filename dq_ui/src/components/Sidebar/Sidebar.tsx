import React, { useState } from 'react';
import { Link,useLocation } from 'react-router-dom';
import { Layout, Menu,Button } from 'antd';
import {
  HomeOutlined,
  FileTextOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import styles from './Sidebar.module.scss'
import {
  MenuFoldOutlined,
  MenuUnfoldOutlined
} from '@ant-design/icons';

const { Sider } = Layout;

const Sidebar: React.FC = () => {
  const location = useLocation();

  const [collapsed, setCollapsed] = useState(false);

  const toggleSidebar = () => {
    setCollapsed(!collapsed);
  };

  const getSelectedKey = () => {
    const pathname = location.pathname;
    if (pathname === '/entity') {
      return '2';
    } else if (pathname === '/ruleset') {
      return '3';
    } else if (pathname === '/rule') {
      return '4';
    } else {
      return '1'; // Default to 'Home'
    }
  };

  return (
    <Sider collapsible collapsed={collapsed} onCollapse={toggleSidebar} 
    collapsedWidth={80} // Adjust the collapsed width as needed
    >
     
      <Button className={ `${styles.logo} ${collapsed ? 'collapsed' : ''}`} type="primary" onClick={toggleSidebar} style={{ marginBottom: 16 }}>
        {collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
      </Button>
      <Menu theme="dark" mode="inline" selectedKeys={[getSelectedKey()]}>
        <Menu.Item key="1" icon={<HomeOutlined />}>
          <Link to="/">Home</Link>
        </Menu.Item>
        <Menu.Item key="2" icon={<FileTextOutlined />}>
          <Link to="/entity">Entity</Link>
        </Menu.Item>
        <Menu.Item key="3" icon={<SettingOutlined />}>
          <Link to="/ruleset">RuleSet</Link>
        </Menu.Item>
        <Menu.Item key="4" icon={<FileTextOutlined />}>
          <Link to="/rule">Rule</Link>
        </Menu.Item>
      </Menu>
    </Sider>
  );
};

export default Sidebar;
