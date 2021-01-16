import { ident, literal } from 'pg-format'
import { schemasSql } from './sql'

export default class PostgresSchemaApi {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async getAll() {
    const { data } = await this.query(schemasSql)
    return data
  }

  async getById(id: number) {
    const sql = `${schemasSql} AND n.oid = ${literal(id)};`
    const {
      data: [schema],
    } = await this.query(sql)
    return schema
  }

  async getByName(name: string) {
    const sql = `${schemasSql} AND n.nspname = ${literal(name)};`
    const {
      data: [schema],
    } = await this.query(sql)
    return schema
  }

  async create({ name, owner = 'postgres' }: { name: string; owner?: string }) {
    const sql = `CREATE SCHEMA IF NOT EXISTS ${ident(name)} AUTHORIZATION ${ident(owner)};`
    await this.query(sql)
    const schema = await this.getByName(name)
    return schema
  }

  async alter(id: number, { name, owner }: { name?: string; owner?: string }) {
    const old = await this.getById(id)
    const nameSql =
      name === undefined ? '' : `ALTER SCHEMA ${ident(old.name)} RENAME TO ${ident(name)};`
    const ownerSql =
      owner === undefined ? '' : `ALTER SCHEMA ${ident(old.name)} OWNER TO ${ident(owner)};`
    const sql = `BEGIN; ${ownerSql} ${nameSql} COMMIT;`
    await this.query(sql)
    const schema = await this.getById(old.id)
    return schema
  }

  async drop(id: number, { cascade = false } = {}) {
    const schema = await this.getById(id)
    const sql = `DROP SCHEMA ${ident(schema.name)} ${cascade ? 'CASCADE' : 'RESTRICT'};`
    await this.query(sql)
    return schema
  }
}
