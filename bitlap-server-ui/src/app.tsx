// 运行时配置

// 全局初始化数据配置，用于 Layout 用户信息和权限初始化
// 更多信息见文档：https://next.umijs.org/docs/api/runtime-config#getinitialstate

import type { Settings as LayoutSettings } from '@ant-design/pro-components';
import { SettingDrawer } from '@ant-design/pro-components';
import { Github, Question, SelectLang } from '@/components/RightContent';
import { RunTimeLayoutConfig } from '@@/plugin-layout/types';
import defaultSettings from '../config/defaultSettings';
import { requestConfig } from './request';
import Footer from '@/components/Footer';
import {
  AvatarDropdown,
  AvatarName,
} from './components/RightContent/AvatarDropdown';
import { getCurrentUserInfo } from '@/services/user/getUserInfo';
import { history } from 'umi';

const loginPath = '/login';

export async function getInitialState(): Promise<{
  settings?: Partial<LayoutSettings>;
  currentUser?: API.CurrentUser;
  loading?: boolean;
  fetchUserInfo?: () => Promise<API.CurrentUser | undefined>;
}> {
  const fetchUserInfo = async () => {
    // 本地调试模式
    if (BITLAP_DEBUG === 'true') {
      return {
        id: BITLAP_IP,
        name: BITLAP_USER || BITLAP_IP,
        avatar:
          'https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png',
      };
    }
    const item = window.sessionStorage.getItem('user');
    const user = item ? JSON.parse(item) : {};
    if (user.id === null) {
      return [];
    }
    const result = await getCurrentUserInfo(user.id);
    return result?.data || [];
  };

  // 如果不是登录页面，执行
  if (location.pathname !== loginPath) {
    const currentUser = await fetchUserInfo();
    return {
      fetchUserInfo,
      currentUser,
      settings: defaultSettings as Partial<LayoutSettings>,
    };
  }
  return {
    fetchUserInfo,
    settings: defaultSettings as Partial<LayoutSettings>,
  };
}

export const layout: RunTimeLayoutConfig = ({
  initialState,
  setInitialState,
}) => {
  const currentUser = initialState?.currentUser;
  return {
    ...initialState?.settings,
    // 去掉面包屑
    // breadcrumbRender: () => [],
    // 去掉默认的实现
    // rightContentRender: false,
    avatarProps: {
      src: currentUser?.avatar,
      title: <AvatarName />,
      render: (_, avatarChildren) => {
        return <AvatarDropdown>{avatarChildren}</AvatarDropdown>;
      },
    },
    waterMarkProps: {
      content: currentUser?.name,
    },
    footerRender: () => <Footer />,
    actionsRender: () => [
      <Question key="Question" />,
      <Github key="Github" />,
      <SelectLang key="SelectLang" />,
    ],
    onPageChange: () => {
      const { location } = history;
      // 如果没有登录，重定向到 login
      if (
        location! &&
        !initialState?.currentUser &&
        location.pathname !== loginPath &&
        BITLAP_DEBUG !== 'true'
      ) {
        history.push(loginPath);
      }
    },
    links: [],
    menuHeaderRender: undefined,
    // 自定义 403 页面
    // unAccessible: <div>unAccessible</div>,
    // 增加一个 loading 的状态
    childrenRender: (children) => {
      // if (initialState?.loading) return <PageLoading />;
      return (
        <>
          {children}
          <SettingDrawer
            disableUrlParams
            enableDarkTheme
            settings={initialState?.settings}
            onSettingChange={(settings) => {
              setInitialState((preInitialState) => ({
                ...preInitialState,
                settings,
              }));
            }}
          />
        </>
      );
    },
  };
};

/**
 * @name request 配置，可以配置错误处理
 * 它基于 axios 和 ahooks 的 useRequest 提供了一套统一的网络请求和错误处理方案。
 * @doc https://umijs.org/docs/max/request#配置
 */
export const request = { ...requestConfig };
