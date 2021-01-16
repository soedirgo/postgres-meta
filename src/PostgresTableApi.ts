import { ident, literal } from 'pg-format'
import { coalesceRowsToArray } from './helpers'
import {
  tablesSql,
  columnsSql,
  grantsSql,
  policiesSql,
  primaryKeysSql,
  relationshipsSql,
} from './sql'

export default class PostgresTableApi {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async getAll() {
    const { data } = await this.query(enrichedTablesSql)
    return data
  }

  async getById(id: number) {
    const sql = `${enrichedTablesSql} WHERE tables.id = ${literal(id)};`
    const {
      data: [table],
    } = await this.query(sql)
    return table
  }

  async getByName(name: string, schema: string) {
    const sql = `${enrichedTablesSql} WHERE tables.name = ${literal(
      name
    )} AND tables.schema = ${literal(schema)};`
    const {
      data: [table],
    } = await this.query(sql)
    return table
  }

  async create({
    name,
    schema = 'public',
    comment,
  }: {
    name: string
    schema?: string
    comment?: string
  }) {
    const tableSql = `CREATE TABLE IF NOT EXISTS ${ident(schema)}.${ident(name)} ();`
    const commentSql =
      comment === undefined
        ? ''
        : `COMMENT ON TABLE ${ident(schema)}.${ident(name)} IS ${literal(comment)};`
    const sql = `BEGIN; ${tableSql} ${commentSql} COMMIT;`
    await this.query(sql)
    const table = await this.getByName(name, schema)
    return table
  }

  async alter(
    id: number,
    {
      name,
      schema,
      rls_enabled,
      rls_forced,
      replica_identity,
      replica_identity_index,
      comment,
    }: {
      name?: string
      schema?: string
      rls_enabled?: boolean
      rls_forced?: boolean
      replica_identity?: 'DEFAULT' | 'INDEX' | 'FULL' | 'NOTHING'
      replica_identity_index?: string
      comment?: string
    }
  ) {
    const old = await this.getById(id)

    let alter = `ALTER TABLE ${ident(old.schema)}.${ident(old.name)}`
    const schemaSql = schema === undefined ? '' : `${alter} SET SCHEMA ${ident(schema)};`
    let nameSql = ''
    if (name !== undefined) {
      const currentSchema = schema === undefined ? old.schema : schema
      nameSql = `ALTER TABLE ${ident(currentSchema)}.${ident(old.name)} RENAME TO ${ident(name)}`
    }
    let enableRls = ''
    if (rls_enabled !== undefined) {
      let enable = `${alter} ENABLE ROW LEVEL SECURITY;`
      let disable = `${alter} DISABLE ROW LEVEL SECURITY;`
      enableRls = rls_enabled ? enable : disable
    }
    let forceRls = ''
    if (rls_forced !== undefined) {
      let enable = `${alter} FORCE ROW LEVEL SECURITY;`
      let disable = `${alter} NO FORCE ROW LEVEL SECURITY;`
      forceRls = rls_forced ? enable : disable
    }
    let replicaSql: string
    if (replica_identity === undefined) {
      replicaSql = ''
    } else if (replica_identity === 'INDEX') {
      replicaSql = `${alter} REPLICA IDENTITY USING INDEX ${replica_identity_index};`
    } else {
      replicaSql = `${alter} REPLICA IDENTITY ${replica_identity};`
    }
    const commentSql =
      comment === undefined
        ? ''
        : `COMMENT ON TABLE ${ident(old.schema)}.${ident(old.name)} IS ${literal(comment)};`
    // nameSql must be below schemaSql
    const sql = `
BEGIN;
  ${enableRls}
  ${forceRls}
  ${replicaSql}
  ${commentSql}
  ${schemaSql}
  ${nameSql}
COMMIT;`
    await this.query(sql)
    const table = await this.getById(old.id)
    return table
  }

  async drop(id: number, { cascade = false } = {}) {
    const table = await this.getById(id)
    const sql = `DROP TABLE ${ident(table.schema)}.${ident(table.name)} ${
      cascade ? 'CASCADE' : 'RESTRICT'
    };`
    await this.query(sql)
    return table
  }
}

const enrichedTablesSql = `
WITH tables AS (${tablesSql}),
  columns AS (${columnsSql}),
  grants AS (${grantsSql}),
  policies AS (${policiesSql}),
  primary_keys AS (${primaryKeysSql}),
  relationships AS (${relationshipsSql})
SELECT
  *,
  ${coalesceRowsToArray('columns', 'SELECT * FROM columns WHERE columns.table_id = tables.id')},
  ${coalesceRowsToArray('grants', 'SELECT * FROM grants WHERE grants.table_id = tables.id')},
  ${coalesceRowsToArray('policies', 'SELECT * FROM policies WHERE policies.table_id = tables.id')},
  ${coalesceRowsToArray(
    'primary_keys',
    'SELECT * FROM primary_keys WHERE primary_keys.table_id = tables.id'
  )},
  ${coalesceRowsToArray(
    'relationships',
    `SELECT
       *
     FROM
       relationships
     WHERE
       (relationships.source_schema :: text = tables.schema AND relationships.source_table_name :: text = tables.name)
       OR (relationships.target_table_schema :: text = tables.schema AND relationships.target_table_name :: text = tables.name)`
  )}
FROM tables`
