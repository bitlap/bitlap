import { Navigate, Outlet } from 'umi';
import { useModel } from '@umijs/max';
import { useEffect } from 'react';
import { stringify } from 'querystring';

export default () => {
  const userSimpleInfo = sessionStorage.getItem('user');
  if (userSimpleInfo !== null) {
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
