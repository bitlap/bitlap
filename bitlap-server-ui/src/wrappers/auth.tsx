import { Navigate, Outlet } from 'umi';
import { useModel } from '@umijs/max';

export default (props) => {
  const userSimpleInfo = window.sessionStorage.getItem('user');
  if (userSimpleInfo != null) {
    const { initialState, setInitialState } = useModel('@@initialState');
    setInitialState((s) => ({ ...s, currentUser: JSON.parse(userSimpleInfo) }));
    return <Outlet />;
  } else {
    return <Navigate to="/login" />;
  }
};
