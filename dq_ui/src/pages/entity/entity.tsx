import { useEffect, useState } from 'react';
import { Table, Button, Modal, Space, message, Popconfirm, Typography, Tag } from 'antd';
import styles from './entity.module.scss';
import EntityForm from '../../components/EntityForm/EntityForm';
import { formatString } from '../../common/utilities/utils';
import NiceModal from '@ebay/nice-modal-react';
import { IEntity, IEntityTemplate } from '../../interfaces';
import EntityDetailsModal from '../../components/EntityDetails/EntityDetailsModal';

const { Text } = Typography;

const Entity = () => {
  const [data, setData] = useState<IEntity[]>([]);
  const [editingEntity, setEditingEntity] = useState<IEntity | null>(null);
  const [isModalVisible, setIsModalVisible] = useState(false);

  useEffect(() => {
    // Load saved enttities from localStorage
    const savedData = localStorage.getItem('entities');
    if (savedData) {
      setData(JSON.parse(savedData));
    }
  }, []);

  const showModal = () => {
    setIsModalVisible(true);
  };

  const handleCancel = () => {
    setEditingEntity(null); // Ensure no entity is set for editing
    setIsModalVisible(false);
  };

  const showEntityDetails = (entity: any) => {
    NiceModal.show(EntityDetailsModal, { entity });
  };

  const editEntityModal = (entity: IEntity) => {
    setEditingEntity(entity); // Set the entity to edit
    setIsModalVisible(true); // Show the form modal
  };

  const deleteEntity = (entity: IEntity) => {
    let entities: string | null | [IEntity] = localStorage.getItem('entities');
    const parsedEntities = JSON.parse(entities || '') || [];
    if (parsedEntities) {
      let filteredEntities = parsedEntities.filter(
        (ent: IEntity) => ent.entityName !== entity.entityName
      );
      localStorage.setItem('entities', JSON.stringify(filteredEntities));
      setData(filteredEntities);
      message.success('Entity deleted successfully!');
    }
  };

  const columns = [
    {
      title: 'Entity Name',
      dataIndex: 'entityName',
      key: 'entityName',
      render: (name: string, record: IEntity) => (
        <>
          <Text>{name}</Text> {record?.isDraft ? <Tag>Draft</Tag> : null}
        </>
      ),
    },
    {
      title: 'Entity Physical Name',
      dataIndex: 'entityPhysicalName',
      key: 'entityPhysicalName',
    },
    {
      title: 'Entity Type',
      dataIndex: 'entity_type',
      key: 'entity_type',
      render: (type: string) => formatString(type),
    },
    {
      title: 'Entity SubType',
      dataIndex: 'entity_subtype',
      key: 'entity_subtype',
      render: (subtype: string) => formatString(subtype),
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (record: IEntity) => (
        <Space size='middle'>
          <a onClick={() => showEntityDetails(record)}>View</a>
          <a
            onClick={() => {
              editEntityModal(record);
            }}
          >
            Edit
          </a>
          <Popconfirm
            title='Delete the Entity'
            description='Are you sure to delete this entity?'
            onConfirm={() => {
              deleteEntity(record);
            }}
            onCancel={() => {}}
            okText='Delete'
            cancelText='Cancel'
          >
            <a> Delete</a>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Button type='primary' onClick={showModal} className={styles.createButton}>
        Create Entity
      </Button>
      <Table columns={columns} dataSource={data} />
      <Modal
        title={editingEntity ? 'Edit Entity' : 'Create Entity'}
        visible={isModalVisible}
        onCancel={handleCancel}
        width={'70%'}
        footer={null}
      >
        <EntityForm
          setIsModalVisible={setIsModalVisible}
          setData={setData}
          entityToEdit={editingEntity}
        />
      </Modal>
    </div>
  );
};

export default Entity;
