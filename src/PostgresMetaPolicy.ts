import { ident, literal } from 'pg-format'
import { policiesSql } from './sql'

export default class PostgresMetaPolicy {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async list() {
    const { data } = await this.query(policiesSql)
    return data
  }

  async retrieve({ id }: { id: number }): Promise<any>
  async retrieve({
    name,
    table,
    schema,
  }: {
    name: string
    table: string
    schema: string
  }): Promise<any>
  async retrieve({
    id,
    name,
    table,
    schema = 'public',
  }: {
    id?: number
    name?: string
    table?: string
    schema?: string
  }) {
    if (id) {
      const sql = `${policiesSql} WHERE pol.oid = ${literal(id)};`
      const {
        data: [policy],
      } = await this.query(sql)
      return policy
    } else if (name && table) {
      const sql = `${policiesSql} WHERE pol.polname = ${literal(name)} AND n.nspname = ${literal(
        schema
      )} AND c.relname = ${literal(table)};`
      const {
        data: [policy],
      } = await this.query(sql)
      return policy
    } else {
      // TODO error
    }
  }

  async create({
    name,
    table,
    schema = 'public',
    definition,
    check,
    action = 'PERMISSIVE',
    command = 'ALL',
    roles = ['PUBLIC'],
  }: {
    name: string
    table: string
    schema?: string
    definition?: string
    check?: string
    action?: string
    command?: string
    roles?: string[]
  }) {
    const definitionClause = definition === undefined ? '' : `USING (${definition})`
    const checkClause = check === undefined ? '' : `WITH CHECK (${check})`
    const sql = `
CREATE POLICY ${ident(name)} ON ${ident(schema)}.${ident(table)}
  AS ${action}
  FOR ${command}
  TO ${roles.join(',')}
  ${definitionClause} ${checkClause};`
    await this.query(sql)
    const policy = await this.retrieve({ name, table, schema })
    return policy
  }

  async update(
    id: number,
    {
      name,
      definition,
      check,
      roles,
    }: {
      name: string
      definition?: string
      check?: string
      roles?: string[]
    }
  ) {
    const old = await this.retrieve({ id })

    const alter = `ALTER POLICY ${ident(old.name)} ON ${ident(old.schema)}.${ident(old.table)}`
    const nameSql = name === undefined ? '' : `${alter} RENAME TO ${ident(name)};`
    const definitionSql = definition === undefined ? '' : `${alter} USING (${definition});`
    const checkSql = check === undefined ? '' : `${alter} WITH CHECK (${check});`
    const rolesSql = roles === undefined ? '' : `${alter} TO (${roles.join(',')});`

    // nameSql must be last
    const sql = `BEGIN; ${definitionSql} ${checkSql} ${rolesSql} ${nameSql} COMMIT;`
    await this.query(sql)
    const policy = await this.retrieve({ id })
    return policy
  }

  async remove(id: number) {
    const policy = await this.retrieve({ id })
    const sql = `DROP POLICY ${ident(policy.name)} ON ${ident(policy.schema)}.${ident(
      policy.table
    )};`
    await this.query(sql)
    return policy
  }
}
