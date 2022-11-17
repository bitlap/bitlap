import { Settings as LayoutSettings } from '@ant-design/pro-components';

const Settings: LayoutSettings & {
  pwa?: boolean;
  logo?: string;
  locale?: boolean;
  breadcrumbRender?: any;
  defaultCollapsed?: boolean;
  breakpoint?: boolean;
  rightContentRender?: any;
  avatarProps?: any;
} = {
  navTheme: 'realDark',
  colorPrimary: '#66ccff',
  layout: 'mix',
  contentWidth: 'Fluid',
  fixedHeader: true,
  fixSiderbar: true,
  splitMenus: false,
  defaultCollapsed: true,
  breakpoint: false,
  colorWeak: false,
  title: 'bitlap',
  pwa: false,
  logo: '/logo.png',
  iconfontUrl: '',
  locale: true,
};

export default Settings;
