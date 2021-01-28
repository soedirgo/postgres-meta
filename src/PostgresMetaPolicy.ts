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
      schema,
      table,
      definition,
      check,
      roles,
    }: {
      name: string
      schema: string
      table: string
      definition?: string
      check?: string
      roles?: string[]
    }
  ) {
    const old = await this.retrieve({ id })

    // TODO
    let alter = `ALTER POLICY ${ident(name)} ON ${ident(schema)}.${ident(table)}`
    let definitionSql = definition === undefined ? '' : `${alter} USING (${definition});`
    let checkSql = check === undefined ? '' : `${alter} WITH CHECK (${check});`
    let rolesSql = roles === undefined ? '' : `${alter} TO (${roles.join(',')});`

    const sql = `BEGIN; ${definitionSql} ${checkSql} ${rolesSql} COMMIT;`
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
