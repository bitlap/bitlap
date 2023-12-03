import { extend } from 'umi-request';
import { notification } from 'antd';

function errorHandler(error) {
  notification.error({
    message: '请求失败',
    description: error.message,
  });
}

const request = extend({
  responseType: 'json',
  errorHandler,
  credentials: 'include',
});

request.interceptors.response.use(async (res, req) => {
  const { code, error } = await res.clone().json();
  const { noThrow } = req || {};
  const msg = error || '未知错误';
  if (res.status !== 200) {
    if (error !== null) {
      throw new Error(`${msg}`);
    } else {
      throw new Error(`${res.statusText}`);
    }
  } else {
    if (code !== 0 && !noThrow) {
      throw new Error(msg);
    }
  }
  return res;
});

export default request;
