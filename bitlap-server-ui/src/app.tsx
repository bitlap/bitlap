// 运行时配置

// 全局初始化数据配置，用于 Layout 用户信息和权限初始化
// 更多信息见文档：https://next.umijs.org/docs/api/runtime-config#getinitialstate

import { RunTimeLayoutConfig } from '@@/plugin-layout/types';
import defaultSettings from '../config/defaultSettings';
import { GithubFilled } from '@ant-design/icons';

export async function getInitialState(): Promise<{
  name: string;
  settings: any;
}> {
  return {
    name: 'admin',
    settings: defaultSettings,
  };
}

export const layout: RunTimeLayoutConfig = ({ initialState }) => {
  return {
    ...initialState?.settings,
    // 去掉面包屑
    breadcrumbRender: () => [],
    // 去掉默认的实现
    rightContentRender: false,
    avatarProps: {
      src: 'https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png',
      size: 'small',
      title: 'admin',
    },
    actionsRender: () => [
      <GithubFilled
        key="GithubFilled"
        onClick={() => window.open('https://github.com/bitlap/bitlap')}
      />,
    ],
  };
};
