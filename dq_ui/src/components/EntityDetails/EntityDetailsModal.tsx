import React from 'react';
import { Descriptions, Modal, Space } from 'antd';
import type { DescriptionsProps } from 'antd';
import NiceModal, { useModal } from '@ebay/nice-modal-react';
import { formatString } from '../../common/utilities/utils';
import { IEntity, IEntityDetailsModalProps } from '../../interfaces';

// Function to convert entity details to description items
const entityToDescriptionsItems = (entity: IEntity): DescriptionsProps['items'] => {
  const baseItems: DescriptionsProps['items'] = [
    {
      key: '2',
      label: 'Entity Type',
      children: formatString(entity.entity_type),
    },
    {
      key: '1',
      label: 'Entity SubType',
      children: formatString(entity.entity_subtype),
    },
    {
      key: '3',
      label: 'Entity Name',
      children: entity.entityName,
    },
    {
      key: '4',
      label: 'Entity Physical Name',
      children: entity.entityPhysicalName,
    },
    {
      key: '5',
      label: 'Primary Key',
      children: entity.primaryKey,
    },
  ];

  // Add entity properties to the items
  const propertyItems: DescriptionsProps['items'] = entity.properties.map((prop, index) => ({
    key: `property-${index}`,
    label: `Property ${index + 1}`,
    children: (
      <Descriptions
        bordered
        items={[
          {
            key: 'prop-${index}',
            label: `${prop.propertyName}`,
            children: prop.propertyValue,
          },
        ]}
      />
    ),
  }));

  return [...baseItems, ...propertyItems];
};

export const EntityDetailsModal = NiceModal.create(({ entity }: IEntityDetailsModalProps) => {
  const modal = useModal();

  const items = entityToDescriptionsItems(entity);

  return (
    <Modal
      title='Entity Details'
      open={modal.visible}
      onCancel={modal.hide}
      footer={null}
      width={800}
    >
      <Descriptions items={items} column={1} />
    </Modal>
  );
});

export default EntityDetailsModal;
