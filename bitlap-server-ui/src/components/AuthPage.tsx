import React from 'react';
import { PageContainer } from '@ant-design/pro-components';
import { useModel } from '@umijs/max';
import { Result } from 'antd';

export type AuthPageProps = {
  title?: boolean;
  auth?: boolean;
  children?: React.ReactNode;
};

const AuthPage: React.FC<AuthPageProps> = ({
  title,
  auth,
  children,
  ...restProps
}) => {
  const { initialState } = useModel('@@initialState');
  const { currentUser } = initialState || {};

  // 以下场景不控制权限
  // > 1. 本地local debug模式
  // > 2. 页面不控制权限
  // > 3. 当前登录用户有 admin 角色
  // @ts-ignore
  if (!auth || BITLAP_DEBUG === 'true' || currentUser?.name === 'root') {
    return (
      <PageContainer title={title} {...restProps}>
        {children}
      </PageContainer>
    );
  }
  return (
    <Result
      status="403"
      title="403"
      subTitle="暂无权限，请联系相关负责人添加权限。"
    />
  );
};

export default AuthPage;
