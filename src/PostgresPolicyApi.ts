import { ident, literal } from 'pg-format'
import { policiesSql } from './sql'

export default class PostgresPolicyApi {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async getAll() {
    const { data } = await this.query(policiesSql)
    return data
  }

  async getById(id: number) {
    const sql = `${policiesSql} WHERE pol.oid = ${literal(id)};`
    const {
      data: [policy],
    } = await this.query(sql)
    return policy
  }

  async getByName(name: string, table: string, schema: string) {
    const sql = `${policiesSql} WHERE pol.polname = ${literal(name)} AND n.nspname = ${literal(
      schema
    )} AND c.relname = ${literal(table)};`
    const {
      data: [policy],
    } = await this.query(sql)
    return policy
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
    const policy = await this.getByName(name, table, schema)
    return policy
  }

  async alter(
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
    const old = await this.getById(id)

    let alter = `ALTER POLICY ${ident(name)} ON ${ident(schema)}.${ident(table)}`
    let definitionSql = definition === undefined ? '' : `${alter} USING (${definition});`
    let checkSql = check === undefined ? '' : `${alter} WITH CHECK (${check});`
    let rolesSql = roles === undefined ? '' : `${alter} TO (${roles.join(',')});`

    const sql = `BEGIN; ${definitionSql} ${checkSql} ${rolesSql} COMMIT;`
    await this.query(sql)
    const policy = await this.getById(old.id)
    return policy
  }

  async drop(id: number) {
    const policy = await this.getById(id)
    const sql = `DROP POLICY ${ident(policy.name)} ON ${ident(policy.schema)}.${ident(
      policy.table
    )};`
    await this.query(sql)
    return policy
  }
}
