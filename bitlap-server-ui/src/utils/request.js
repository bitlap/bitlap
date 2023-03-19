import {extend} from 'umi-request'
import {notification} from 'antd';

function errorHandler(error) {
    notification.error({
        message: '请求错误', description: error.message,
    });
}


const request = extend({
    responseType: 'json', errorHandler
});

request.interceptors.response.use(async (res, req) => {
    const {resultCode, errorMessage} = await res.clone().json()
    const {noThrow} = req || {}
    const msg = errorMessage || "未知错误"
    if (res.status !== 200) {
        throw new Error(`${res.statusText}`);
    } else {
        if (resultCode !== 0 && !noThrow) {
            throw new Error(msg)
        }
    }
    return res
})

export default request