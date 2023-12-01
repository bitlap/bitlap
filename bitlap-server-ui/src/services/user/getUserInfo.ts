// @ts-ignore
import { request } from '@umijs/max';

export async function getCurrentUserInfo(
  name: string,
  options?: { [key: string]: any },
) {
  return request<API.CurrentUser>('/api/user/getUserByName', {
    method: 'GET',
    params: {
      name: name,
    },
    ...(options || {}),
  });
}
