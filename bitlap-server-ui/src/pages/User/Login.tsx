import {
    LockOutlined,
    UserOutlined,

} from '@ant-design/icons';
import {
    LoginFormPage,
    ProConfigProvider,
    ProFormText,
} from '@ant-design/pro-components';
import type {CSSProperties} from 'react';
import React from 'react';
import {Form, theme} from 'antd';
import {accountLogin} from '@/services/user/login';
import {
    LoginForm,

} from '@ant-design/pro-components';
import {history} from 'umi'
import {Toast} from 'antd-mobile';
import {useModel} from "@@/exports";

const iconStyles: CSSProperties = {
    color: 'rgba(0, 0, 0, 0.2)',
    fontSize: '18px',
    verticalAlign: 'middle',
    cursor: 'pointer',
};


export default () => {
    const [form] = Form.useForm();
    const handleSubmit = async (values) => {
        const result = await accountLogin({...values});
        const data = result?.data || [];
        window.sessionStorage.setItem('user', JSON.stringify(data));
        Toast.show({
            icon: 'success',
            content: '登录成功',
        })

        history.push({pathname: '/pages/welcome'})
    };
    const {token} = theme.useToken();
    return (
        <ProConfigProvider hashed={false}>
            <div style={{backgroundColor: token.colorBgContainer}}>
                <LoginForm
                    logo="https://avatars.githubusercontent.com/u/74587793?s=200&v=4"
                    title="Bitlap"
                    subTitle="Bitlap Platform"
                    onFinish={handleSubmit}
                    initialValues={{}}
                    form={form}>
                    <ProFormText
                        name="username"
                        fieldProps={{
                            size: 'large',
                            prefix: <UserOutlined className={'prefixIcon'}/>,
                        }}
                        placeholder={'用户名: admin or user'}
                        rules={[
                            {
                                required: true,
                                message: '请输入用户名!',
                            },
                        ]}
                    />
                    <ProFormText.Password
                        name="password"
                        fieldProps={{
                            size: 'large',
                            prefix: <LockOutlined className={'prefixIcon'}/>,
                            strengthText:
                                'Password should contain numbers, letters and special characters, at least 8 characters long.',

                            statusRender: (value) => {
                                const getStatus = () => {
                                    if (value && value.length > 12) {
                                        return 'ok';
                                    }
                                    if (value && value.length > 6) {
                                        return 'pass';
                                    }
                                    return 'poor';
                                };
                                const status = getStatus();
                                if (status === 'pass') {
                                    return (
                                        <div style={{color: token.colorWarning}}>
                                            强度：中
                                        </div>
                                    );
                                }
                                if (status === 'ok') {
                                    return (
                                        <div style={{color: token.colorSuccess}}>
                                            强度：强
                                        </div>
                                    );
                                }
                                return (
                                    <div style={{color: token.colorError}}>强度：弱</div>
                                );
                            },
                        }}
                        placeholder={'密码: ant.design'}
                        rules={[
                            {
                                required: true,
                                message: '请输入密码！',
                            },
                        ]}
                    />
                <div
                    style={{
                        marginBlockEnd: 24,
                    }}
                >
                </div>
            </LoginForm>
        </div>
</ProConfigProvider>
)
    ;
}


