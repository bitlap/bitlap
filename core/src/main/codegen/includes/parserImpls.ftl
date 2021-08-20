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

SqlCreate SqlCreateExtended(Span s, boolean replace) :
{
    final SqlCreate create;
}
{
    (
        create = SqlCreateSchema(s, replace)
    )
    {
        return create;
    }
}

/**
 * Parses a "IF NOT EXISTS" option, default is false.
 */
boolean IfNotExistsOpt() :
{
}
{
    (
        LOOKAHEAD(3)
        <IF> <NOT> <EXISTS> { return true; }
    |
        { return false; }
    )
}

/**
 * Parse tree for {@code CREATE SCHEMA} statement.
 */
SqlCreate SqlCreateSchema(Span s, boolean replace) :
{
    SqlIdentifier schemaName;
    boolean ifNotExists = false;
}
{
    <SCHEMA>

    ifNotExists = IfNotExistsOpt()

    schemaName = CompoundIdentifier()
    {
        return SqlDdlNodes.createSchema(s.pos(), replace, ifNotExists, schemaName);
    }
}
