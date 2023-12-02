import { Navigate, Outlet } from 'umi';
import { useModel } from '@umijs/max';
import { useEffect } from 'react';

export default () => {
  const userSimpleInfo = window.sessionStorage.getItem('user');
  const token = window.sessionStorage.getItem('token');
  if (userSimpleInfo !== null && token !== null) {
    const { initialState, setInitialState } = useModel('@@initialState');
    useEffect(() => {
      setInitialState((s) => ({
        ...s,
        currentUser: JSON.parse(userSimpleInfo),
        loading: false,
      }));
    });
    console.log('userinfo' + JSON.stringify(initialState?.currentUser || {}));
    return <Outlet />;
  } else {
    return <Navigate to="/pages/user/login" />;
  }
};
