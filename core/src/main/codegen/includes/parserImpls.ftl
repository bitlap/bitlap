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

SqlNode SqlRunExampleNode() :
{
  SqlNode stringNode;
}
{
  <RUN> <EXAMPLE>
  stringNode = StringLiteral()
  {
    return new SqlRunExampleNode(getPos(), token.image);
  }
}

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
        create = SqlCreateSchema(s, replace)
        |
        create = SqlCreateDataSource(s, replace)
    )
    {
        return create;
    }
}

SqlCreate SqlCreateSchema(Span s, boolean replace) :
{
    SqlIdentifier schemaName;
    boolean ifNotExists = false;
}
{
    (
        <SCHEMA> | <DATABASE>
    )
    ifNotExists = IfNotExistsOpt()
    schemaName = CompoundIdentifier()
    {
        return new SqlCreateSchema(s.pos(), schemaName, ifNotExists, replace);
    }
}

SqlCreate SqlCreateDataSource(Span s, boolean replace) :
{
    SqlIdentifier dsName;
    boolean ifNotExists = false;
}
{
    (
        <DATASOURCE> | <TABLE>
    )
    ifNotExists = IfNotExistsOpt()
    dsName = CompoundIdentifier()
    {
        return new SqlCreateDataSource(s.pos(), dsName, ifNotExists, replace);
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
        drop = SqlDropSchema(s, replace)
        |
        drop = SqlDropDataSource(s, replace)
    )
    {
        return drop;
    }
}

SqlDrop SqlDropSchema(Span s, boolean replace) :
{
    SqlIdentifier schemaName = null;
    boolean ifExists = false;
}
{
    (
        <SCHEMA> | <DATABASE>
    )
    ifExists = IfExistsOpt()
    schemaName = CompoundIdentifier()
    {
        return new SqlDropSchema(s.pos(), schemaName, ifExists);
    }
}

SqlDrop SqlDropDataSource(Span s, boolean replace) :
{
    SqlIdentifier dsName = null;
    boolean ifExists = false;
}
{
    (
        <DATASOURCE> | <TABLE>
    )
    ifExists = IfExistsOpt()
    dsName = CompoundIdentifier()
    {
        return new SqlDropDataSource(s.pos(), dsName, ifExists);
    }
}

/**
 * ********************************* SQL SHOW *********************************
 */
SqlShowSchemas SqlShowSchemas() :
{
}
{
    <SHOW> ( <SCHEMAS> | <DATABASES> )
    {
        return new SqlShowSchemas(getPos());
    }
}

SqlShowDataSources SqlShowDataSources() :
{
}
{
    <SHOW> ( <DATASOURCES> | <TABLES> )
    {
        return new SqlShowDataSources(getPos());
    }
}
