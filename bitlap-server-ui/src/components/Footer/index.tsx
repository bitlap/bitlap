import { DefaultFooter } from '@ant-design/pro-components';
import { useIntl } from '@umijs/max';
import React from 'react';

const Footer: React.FC = () => {
  const intl = useIntl();
  const defaultMessage = intl.formatMessage({
    id: 'app.copyright.produced',
    defaultMessage: 'bitlap.org',
  });

  const currentYear = new Date().getFullYear();

  return (
    <DefaultFooter
      style={{
        background: 'none',
      }}
      copyright={
        <span>
          {currentYear} {defaultMessage}, Licensed under the &nbsp;
          <a
            href="https://www.apache.org/licenses/"
            target="_blank"
            rel="noopener noreferrer"
          >
            Apache License, Version 2.0.
          </a>
        </span>
      }
      links={[
        {
          key: 'bitlap.org',
          title: 'bitlap.org',
          href: 'https://bitlap.org',
          blankTarget: true,
        },
      ]}
    />
  );
};

export default Footer;
