export default (initialState: { currentUser?: API.CurrentUser }) => {
  // 在这里按照初始化数据定义项目中的权限，统一管理
  // 参考文档 https://next.umijs.org/docs/max/access
  const currentUser = initialState?.currentUser;
  const canSeeAdmin = !!(currentUser && currentUser.name !== 'dontHaveAccess');
  return {
    canSeeAdmin,
  };
};
