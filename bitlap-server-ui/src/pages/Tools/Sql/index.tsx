import React from 'react';
import { PageContainer, ProTable } from '@ant-design/pro-components';
import styles from './index.less';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { Button } from 'antd';
import { EditorView } from '@codemirror/view';
import services from '@/services/sql';

const { runSql } = services.SqlController;

const sqlQuery = async (editor: EditorView, sqlText: string, setColumns: any, setData: any) => {
  if (!sqlText.trim()) return;
  let sql = sqlText;
  // if sql is selected
  const range = editor.state.selection.main;
  if (range.from !== range.to) {
    sql = sqlText.substring(range.from, range.to);
  }
  // run sql
  const res = await runSql(sql);
  const data = res?.data || [];

  // @ts-ignore
  setColumns(data?.columns?.map(c => ({ title: c.name, dataIndex: c.name, valueType: 'text'})) || [])
  // @ts-ignore
  setData(data?.rows?.map(r => r.cells) || []);
};

const SqlPage: React.FC = () => {
  const [editor, setEditor] = React.useState<EditorView>();
  const [sqlText, setSqlText] = React.useState('');
  const [data, setData] = React.useState([]);
  const [columns, setColumns] = React.useState([]);

  return (
    <PageContainer ghost>
      <Button
        type="primary"
        size="small"
        onClick={() => sqlQuery(editor!!, sqlText, setColumns, setData)}
      >
        运行
      </Button>
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
    </PageContainer>
  );
};

export default SqlPage;
