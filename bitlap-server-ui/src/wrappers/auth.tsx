import { Navigate, Outlet } from 'umi';
import { useModel } from '@umijs/max';
import { useEffect } from 'react';

export default () => {
  const userSimpleInfo = window.sessionStorage.getItem('user');
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
