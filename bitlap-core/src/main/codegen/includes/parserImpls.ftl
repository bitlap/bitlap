<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

/**
 * Parses a "IF EXISTS" option, default is false.
 */
boolean IfExistsOpt() :
{
}
{
    (
        LOOKAHEAD(2)
        <IF> <EXISTS> { return true; }
    |
        { return false; }
    )
}

/**
 * Parses a "IF NOT EXISTS" option, default is false.
 */
boolean IfNotExistsOpt() :
{
}
{
    (
        LOOKAHEAD(2)
        <IF> <NOT> <EXISTS> { return true; }
    |
        { return false; }
    )
}

/**
 * ********************************* SQL CREATE *********************************
 */

/**
 * Common parser for create statements.
 */
SqlCreate SqlCreateExtended(Span s, boolean replace) :
{
    final SqlCreate create;
}
{
    (
        create = SqlCreateDatabase(s, replace)
        |
        create = SqlCreateTable(s, replace)
        |
        create = SqlCreateUser(s, replace)                    
    )
    {
        return create;
    }
}

SqlCreate SqlCreateDatabase(Span s, boolean replace) :
{
    SqlIdentifier dbName;
    boolean ifNotExists = false;
}
{
    (
        <SCHEMA> | <DATABASE>
    )
    ifNotExists = IfNotExistsOpt()
    dbName = CompoundIdentifier()
    {
        return new SqlCreateDatabase(s.pos(), dbName, ifNotExists, replace);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    SqlIdentifier tableName;
    boolean ifNotExists = false;
}
{
    (
        <DATASOURCE> | <TABLE>
    )
    ifNotExists = IfNotExistsOpt()
    tableName = CompoundIdentifier()
    {
        return new SqlCreateTable(s.pos(), tableName, ifNotExists, replace);
    }
}


/**
 * ********************************* SQL DROP *********************************
 */

/**
 * Common parser for drop statements.
 */
SqlDrop SqlDropExtended(Span s, boolean replace) :
{
    final SqlDrop drop;
}
{
    (
        drop = SqlDropDatabase(s, replace)
        |
        drop = SqlDropTable(s, replace)
        |
        drop = SqlDropUser(s, replace)                
    )
    {
        return drop;
    }
}

SqlDrop SqlDropDatabase(Span s, boolean replace) :
{
    SqlIdentifier dbName = null;
    boolean ifExists = false;
    boolean cascade = false;
}
{
    (
        <SCHEMA> | <DATABASE>
    )
    ifExists = IfExistsOpt()
    dbName = CompoundIdentifier()
    [
      <CASCADE> { cascade = true; }
    ]
    {
        return new SqlDropDatabase(s.pos(), dbName, ifExists, cascade);
    }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    SqlIdentifier tableName = null;
    boolean ifExists = false;
    boolean cascade = false;
}
{
    (
        <DATASOURCE> | <TABLE>
    )
    ifExists = IfExistsOpt()
    tableName = CompoundIdentifier()
    [
      <CASCADE> { cascade = true; }
    ]
    {
        return new SqlDropTable(s.pos(), tableName, ifExists, cascade);
    }
}

/**
 * ********************************* SQL SHOW *********************************
 */
SqlShowDatabases SqlShowDatabases() :
{
    SqlIdentifier pattern = null;
}
{
    <SHOW> ( <SCHEMAS> | <DATABASES> )
    [
        <LIKE> pattern = CompoundIdentifier()
    ]
    {
        return new SqlShowDatabases(getPos(), pattern);
    }
}

SqlShowTables SqlShowTables() :
{
    SqlIdentifier dbName = null;
}
{
    <SHOW> ( <DATASOURCES> | <TABLES> )
    [
        <IN> dbName = CompoundIdentifier()
    ]
    {
        return new SqlShowTables(getPos(), dbName);
    }
}
            
SqlNode SqlUseDatabase() :
{
    SqlIdentifier dbName = null;
}
{
    <USE>
    dbName = CompoundIdentifier()
    {
        return new SqlUseDatabase(getPos(), dbName);
    }
}
        
SqlNode SqlShowCurrentDatabase() :
{
}
{
    <SHOW> <CURRENT_DATABASE>
    {
        return new SqlShowCurrentDatabase(getPos());
    }
}        
            
SqlNode SqlExplainX() :
{
  SqlNode stmt;
}
{
  <EXPLAIN>
  stmt = SqlQueryOrDml()
  {
    return new SqlExplainX(getPos(), stmt);
  }
}


/**
 * ********************************* other commands *********************************
 */
 SqlNode SqlRunExample() :
 {
   SqlNode stringNode;
 }
 {
   <RUN> <EXAMPLE>
   stringNode = StringLiteral()
   {
     return new SqlRunExample(getPos(), token.image);
   }
 }

 SqlNode SqlLoadData() :
 {
   SqlNode filePath;
   SqlIdentifier tableName;
   boolean overwrite = false;
 }
 {
   <LOAD> <DATA> filePath = StringLiteral()
   (<INTO> | <OVERWRITE> { overwrite = true; })
   <TABLE> tableName = CompoundIdentifier()
   {
     return new SqlLoadData(getPos(), filePath, tableName, overwrite);
   }
 }

/**
 * ********************************* user commands *********************************
 */
SqlShowUsers SqlShowUsers() :
{

}
{
    <SHOW> <USERS>
    {
        return new SqlShowUsers(getPos());
    }
}

SqlCreate SqlCreateUser(Span s, boolean replace) :
{
    boolean ifNotExists = false;
    SqlIdentifier name;
    String password = null;
}
{
    <USER>
    ifNotExists = IfNotExistsOpt()
    name = CompoundIdentifier()
    [
        <IDENTIFIED> <BY> { password = SimpleStringLiteral(); }
    ]
    {
        return new SqlCreateUser(s.pos(), name, password, ifNotExists, replace);
    }
}       

            
SqlDrop SqlDropUser(Span s, boolean replace) :
{
    boolean ifExists = false;
    SqlIdentifier name = null;
}
{
    <USER>
    ifExists = IfExistsOpt()
    name = CompoundIdentifier()
    {
        return new SqlDropUser(s.pos(), name, ifExists);
    }
}