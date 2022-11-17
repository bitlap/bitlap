export default [
  {
    path: '/',
    redirect: '/pages/home',
  },
  {
    name: 'Home',
    path: '/pages/home',
    icon: 'HomeOutlined',
    component: './Home',
  },
  {
    name: 'Tools',
    path: '/pages/tools',
    icon: 'RocketOutlined',
    routes: [
      {
        path: '/pages/tools/sql',
        name: 'SQL',
        component: './Tools/Sql',
      },
    ],
  },
];
