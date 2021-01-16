import { ident, literal } from 'pg-format'
import { columnsSql } from './sql'
import PostgresTableApi from './PostgresTableApi'

export default class PostgresColumnApi {
  query: Function
  tableApi: PostgresTableApi

  constructor(query: Function) {
    this.query = query
    this.tableApi = new PostgresTableApi(query)
  }

  async getAll() {
    const { data } = await this.query(columnsSql)
    return data
  }

  async getById(id: string) {
    const regexp = /^(\d+)\.(\d+)$/
    if (!regexp.test(id)) {
      throw new Error('Invalid format for column ID.')
    }
    const matches = id.match(regexp) as RegExpMatchArray
    const [tableId, ordinalPos] = matches.slice(1).map(Number)
    const sql = `${columnsSql} AND c.oid = ${tableId} AND a.attnum = ${ordinalPos}`
    const {
      data: [column],
    } = await this.query(sql)
    return column
  }

  async getByName(tableId: number, name: string) {
    const sql = `${columnsSql} AND c.oid = ${tableId} AND a.attname = ${name}`
    const {
      data: [column],
    } = await this.query(sql)
    return column
  }

  async create({
    tableId,
    name,
    type,
    default_value,
    default_value_format = 'literal',
    is_identity = false,
    identity_generation = 'BY DEFAULT',
    is_nullable = true,
    is_primary_key = false,
    is_unique = false,
    comment,
  }: {
    tableId: number
    name: string
    type: string
    default_value?: any
    default_value_format?: 'expression' | 'literal'
    is_identity?: boolean
    identity_generation?: 'BY DEFAULT' | 'ALWAYS'
    is_nullable?: boolean
    is_primary_key?: boolean
    is_unique?: boolean
    comment?: string
  }) {
    const { name: table, schema } = await this.tableApi.getById(tableId)

    let defaultValueClause: string
    if (default_value === undefined) {
      defaultValueClause = ''
    } else if (default_value_format === 'expression') {
      defaultValueClause = `DEFAULT ${default_value}`
    } else {
      defaultValueClause = `DEFAULT ${literal(default_value)}`
    }
    const isIdentityClause = is_identity ? `GENERATED ${identity_generation} AS IDENTITY` : ''
    const isNullableClause = is_nullable ? 'NULL' : 'NOT NULL'
    const isPrimaryKeyClause = is_primary_key ? 'PRIMARY KEY' : ''
    const isUniqueClause = is_unique ? 'UNIQUE' : ''
    const commentSql =
      comment === undefined
        ? ''
        : `COMMENT ON COLUMN ${ident(schema)}.${ident(table)}.${ident(name)} IS ${literal(comment)}`

    const sql = `
BEGIN;
  ALTER TABLE ${ident(schema)}.${ident(table)} ADD COLUMN ${ident(name)} ${type}
    ${defaultValueClause}
    ${isIdentityClause}
    ${isNullableClause}
    ${isPrimaryKeyClause}
    ${isUniqueClause};
  ${commentSql};
COMMIT;`
    await this.query(sql)
    const column = await this.getByName(tableId, name)
    return column
  }

  async alter(
    id: string,
    {
      name,
      type,
      drop_default = false,
      default_value,
      default_value_format = 'literal',
      is_identity,
      identity_generation,
      is_nullable,
      comment,
    }: {
      name?: string
      type?: string
      drop_default?: boolean
      default_value?: any
      default_value_format?: 'expression' | 'literal'
      is_identity?: boolean
      identity_generation?: 'BY DEFAULT' | 'ALWAYS'
      is_nullable?: boolean
      comment?: string
    }
  ) {
    const old = await this.getById(id)

    const nameSql =
      name === undefined || name === old.name
        ? ''
        : `ALTER TABLE ${old.schema}.${old.table} RENAME COLUMN ${old.name} TO ${name};`
    // We use USING to allow implicit conversion of incompatible types (e.g. int4 -> text).
    const typeSql =
      type === undefined
        ? ''
        : `ALTER TABLE ${old.schema}.${old.table} ALTER COLUMN ${old.name} SET DATA TYPE ${type} USING ${old.name}::${type};`

    let defaultValueSql: string
    if (drop_default) {
      defaultValueSql = `ALTER TABLE ${old.schema}.${old.table} ALTER COLUMN ${old.name} DROP DEFAULT;`
    } else if (default_value === undefined) {
      defaultValueSql = ''
    } else {
      const defaultValue =
        default_value_format === 'expression' ? default_value : literal(default_value)
      defaultValueSql = `ALTER TABLE ${old.schema}.${old.table} ALTER COLUMN ${old.name} SET DEFAULT ${defaultValue};`
    }
    // What identitySql does vary depending on the old and new values of
    // is_identity and identity_generation.
    //
    // | is_identity: old \ new | undefined          | true               | false          |
    // |------------------------+--------------------+--------------------+----------------|
    // | true                   | maybe set identity | maybe set identity | drop if exists |
    // |------------------------+--------------------+--------------------+----------------|
    // | false                  | -                  | add identity       | drop if exists |
    let identitySql = `ALTER TABLE ${ident(old.schema)}.${ident(old.table)} ALTER COLUMN ${ident(
      old.name
    )};`
    if (is_identity === false) {
      identitySql += 'DROP IDENTITY IF EXISTS;'
    } else if (old.is_identity === true) {
      if (identity_generation === undefined) {
        identitySql = ''
      } else {
        identitySql += `SET GENERATED ${identity_generation};`
      }
    } else if (is_identity === undefined) {
      identitySql = ''
    } else {
      identitySql += `ADD GENERATED ${identity_generation} AS IDENTITY;`
    }
    let isNullableSql: string
    if (is_nullable === undefined) {
      isNullableSql = ''
    } else {
      isNullableSql = is_nullable
        ? `ALTER TABLE ${old.schema}.${old.table} ALTER COLUMN ${old.name} DROP NOT NULL;`
        : `ALTER TABLE ${old.schema}.${old.table} ALTER COLUMN ${old.name} SET NOT NULL;`
    }
    const commentSql =
      comment === undefined
        ? ''
        : `COMMENT ON COLUMN ${old.schema}.${old.table}.${old.name} IS ${comment};`

    // nameSql must be last.
    // defaultValueSql must be after typeSql.
    // TODO: Can't set default if column is previously identity even if is_identity: false.
    // Must do two separate PATCHes (once to drop identity and another to set default).
    const sql = `
BEGIN;
  ${isNullableSql}
  ${typeSql}
  ${defaultValueSql}
  ${identitySql}
  ${commentSql}
  ${nameSql}
COMMIT;`
    await this.query(sql)
    const column = await this.getById(old.id)
    return column
  }

  async drop(id: string) {
    const column = await this.getById(id)
    const sql = `ALTER TABLE ${column.schema}.${column.table} DROP COLUMN ${column.name};`
    await this.query(sql)
    return column
  }
}
