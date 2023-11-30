// @ts-ignore
import {request} from '@umijs/max';

export async function accountLogout(
    id: string , // with password
    options?: { [key: string]: any },
) {
    return request<API.LoginResult>('/api/user/logout', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        data: {
            id: id
        },
        ...(options || {}),
    });
}
