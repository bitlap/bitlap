import { Navigate, Outlet } from 'umi';
import { useModel } from '@umijs/max';
import { useEffect } from 'react';
import { message } from 'antd';

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
    return <Outlet />;
  } else {
    return <Navigate to="/login" />;
  }
};
