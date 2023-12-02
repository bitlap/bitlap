// @ts-ignore
import { request } from '@umijs/max';

export async function getCurrentUserInfo(
  username: string,
  options?: { [key: string]: any },
) {
  return request<API.GetUserInfoResult>('/api/user/getUserByName', {
    method: 'GET',
    params: {
      username: username,
    },
    ...(options || {}),
  });
}
