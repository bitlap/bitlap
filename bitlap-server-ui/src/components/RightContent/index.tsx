import { HeartTwoTone, QuestionCircleOutlined, EyeOutlined, EyeInvisibleOutlined, GithubFilled } from '@ant-design/icons';
import { SelectLang as UmiSelectLang } from '@umijs/max';
import React, { useState } from 'react';
import ReactMarkdown from "react-markdown";
import { Modal } from "antd";
import './index.less';

export type SiderTheme = 'light' | 'dark';

export const MenuVisible = () => {
  const localStorageEnable = navigator.cookieEnabled && typeof window.localStorage !== 'undefined'
  let visible = "false";
  if (localStorageEnable) {
    visible = window.localStorage.getItem("BITLAP_MENU_VISIBLE") || "false"
  }
  return (
    <div
      title="隐藏无权限的菜单"
      style={{
        display: 'flex',
        height: 26,
      }}
      onClick={() => {
        if (localStorageEnable) {
          // window.localStorage.setItem('umi_locale', lang || '');
          let visible = window.localStorage.getItem("BITLAP_MENU_VISIBLE") || "false"
          if (visible === "true") {
            visible = "false"
          } else {
            visible = "true"
          }
          window.localStorage.setItem("BITLAP_MENU_VISIBLE", visible)
          window.location.reload()
        }
      }}
    >
      { visible === "true" ? <EyeOutlined /> : <EyeInvisibleOutlined /> }
    </div>
  );
}

export const SelectLang = () => {
  return (
    <UmiSelectLang
      style={{
        padding: 4,
      }}
    />
  );
};

export const Question = () => {
  return (
    <div
      title="帮助文档"
      style={{
        display: 'flex',
        height: 26,
      }}
      onClick={() => window.open('https://bitlap.org')}
    >
      <QuestionCircleOutlined />
    </div>
  );
};

export const Github = () => {
  return (
    <GithubFilled
      key="GithubFilled"
      onClick={() => window.open('https://github.com/bitlap/bitlap')}
    />
  )
}



const markdown = `

## 2023-10-01

---

`

export const UpdateLogs = () => {
  const [open, setOpen] = useState(false);
  return (
    <>
      <div
        className="update-log"
        title="更新日志"
        style={{
          display: 'flex',
          height: 26,
        }}
        onClick={() => {
          setOpen(true)
        }}
      >
        <HeartTwoTone twoToneColor="#eb2f96" />
      </div>
      <Modal
        className="update-log-content"
        title="更新日志"
        open={open}
        onOk={() => setOpen(false)}
        onCancel={() => setOpen(false)}
        footer={null}
        width={800}
      >
        <ReactMarkdown>{markdown}</ReactMarkdown>
      </Modal>
    </>
  );
}

