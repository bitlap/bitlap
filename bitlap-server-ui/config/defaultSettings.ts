import { ProLayoutProps } from '@ant-design/pro-components';

const Settings: ProLayoutProps & {
  pwa?: boolean;
  logo?: string;
} = {
  navTheme: 'light',
  colorPrimary: '#66ccff',
  layout: 'mix', // side
  contentWidth: 'Fluid',
  fixedHeader: true,
  fixSiderbar: true,
  colorWeak: false,
  splitMenus: true,
  siderMenuType: 'group', // sub
  breakpoint: false,
  title: 'Bitlap',
  pwa: true,
  logo: '/logo.png',
  iconfontUrl: '',
  token: {
    // 参见ts声明，demo 见文档，通过token 修改样式
    //https://procomponents.ant.design/components/layout#%E9%80%9A%E8%BF%87-token-%E4%BF%AE%E6%94%B9%E6%A0%B7%E5%BC%8F
  },
  // locale: 'zh-CN', // "zh-CN" | "zh-TW" | "en-US" | "it-IT" | "ko-KR"
  menuDataRender: (menuData) => {
    return menuData.map((m) => {
      if (!!m.fixPath) {
        m.path = m.fixPath;
      }
      return m;
    });
  },
};

export default Settings;
