import { Navigate, Outlet } from 'umi';

export default () => {
  const userSimpleInfo = window.sessionStorage.getItem('user');
  const token = window.sessionStorage.getItem('token');
  console.log('userinfo:' + userSimpleInfo);
  console.log('token:' + token);
  if (userSimpleInfo !== null && token !== null) {
    return <Outlet />;
  } else {
    return <Navigate to="/pages/user/login" />;
  }
};
