import React, { useState } from 'react';
import { ProTable } from '@ant-design/pro-components';
import styles from './index.less';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { Button, Spin } from 'antd';
import { EditorView } from '@codemirror/view';
import { runSql } from '@/services/sql';
import AuthPage from '@/components/AuthPage';

const sqlQuery = async (
  editor: EditorView,
  sqlText: string,
  setColumns: any,
  setData: any,
  setLoad: {
    (value: React.SetStateAction<boolean>): void;
    (arg0: boolean): void;
  },
) => {
  if (!sqlText.trim()) return;
  setLoad(true);
  let sql = sqlText;
  // if sql is selected
  const range = editor.state.selection.main;
  if (range.from !== range.to) {
    sql = sqlText.substring(range.from, range.to);
  }
  // run sql
  const res = await runSql({ sql });
  const data = res?.data || [];

  // @ts-ignore
  setColumns(
    // @ts-ignore
    data?.columns?.map((c) => ({
      title: c.name,
      dataIndex: c.name,
      valueType: 'text',
    })) || [],
  );
  // @ts-ignore
  setData(data?.rows?.map((r) => r.cells) || []);
  setLoad(false);
};

const SqlPage: React.FC = () => {
  const [editor, setEditor] = useState<EditorView>();
  const [load, setLoad] = useState(false);

  const [sqlText, setSqlText] = useState('');
  const [data, setData] = useState([]);
  const [columns, setColumns] = useState([]);

  return (
    <AuthPage title={false}>
      <Button
        type="primary"
        size="small"
        onClick={() =>
          sqlQuery(editor!!, sqlText, setColumns, setData, setLoad)
        }
      >
        运行
      </Button>
      <Spin spinning={load}>
        <CodeMirror
          className={styles.editor}
          onCreateEditor={(e) => setEditor(e)}
          value=""
          height="400px"
          extensions={[sql({})]}
          onChange={(value) => setSqlText(value)}
        />
        <ProTable
          headerTitle=""
          rowKey="id"
          search={false}
          toolBarRender={false}
          columns={columns}
          dataSource={data}
        />
      </Spin>
    </AuthPage>
  );
};

export default SqlPage;
