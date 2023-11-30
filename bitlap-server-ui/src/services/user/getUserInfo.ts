// @ts-ignore
import { request } from '@umijs/max';

export async function getCurrentUserInfo(
  id: string,
  options?: { [key: string]: any },
) {
  return request<API.CurrentUser>('/api/user/getUserById', {
    method: 'POST',
    data: {
      id: id,
    },
    ...(options || {}),
  });
}
