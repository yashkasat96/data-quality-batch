// App.tsx
import React from 'react';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import { Layout } from 'antd';
import NiceModal from '@ebay/nice-modal-react';
import Entity from './pages/entity/entity';
import Rule from './pages/rule/rule';
import RuleSet from './pages/ruleSet/ruleset';
import Sidebar from './components/Sidebar/Sidebar';
import LandingPage from "./pages/landingPage/landingPage";

const { Content } = Layout;

const App: React.FC = () => {
  return (
    <Router>
      <Layout style={{ minHeight: '100vh' }}>
        <Sidebar />
        <Layout>
          <Content style={{ margin: '16px' }}>
            <NiceModal.Provider>
              <Routes>
                <Route path="/" element={<LandingPage />} />
                <Route path="/entity" element={<Entity />} />
                <Route path="/ruleset" element={<RuleSet />} />
                <Route path="/rule" element={<Rule />} />
              </Routes>
            </NiceModal.Provider>
          </Content>
        </Layout>
      </Layout>
    </Router>
  );
};

export default App;
