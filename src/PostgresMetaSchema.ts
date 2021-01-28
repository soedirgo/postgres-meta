import { ident, literal } from 'pg-format'
import { schemasSql } from './sql'

export default class PostgresMetaSchema {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async list() {
    const { data } = await this.query(schemasSql)
    return data
  }

  async retrieve({ id }: { id: number }): Promise<any>
  async retrieve({ name }: { name: string }): Promise<any>
  async retrieve({ id, name }: { id?: number; name?: string }) {
    if (id) {
      const sql = `${schemasSql} AND n.oid = ${literal(id)};`
      const {
        data: [schema],
      } = await this.query(sql)
      return schema
    } else if (name) {
      const sql = `${schemasSql} AND n.nspname = ${literal(name)};`
      const {
        data: [schema],
      } = await this.query(sql)
      return schema
    } else {
      // TODO error
    }
  }

  async create({ name, owner = 'postgres' }: { name: string; owner?: string }) {
    const sql = `CREATE SCHEMA IF NOT EXISTS ${ident(name)} AUTHORIZATION ${ident(owner)};`
    await this.query(sql)
    const schema = await this.retrieve({ name })
    return schema
  }

  async update(id: number, { name, owner }: { name?: string; owner?: string }) {
    const old = await this.retrieve({ id })
    const nameSql =
      name === undefined ? '' : `ALTER SCHEMA ${ident(old.name)} RENAME TO ${ident(name)};`
    const ownerSql =
      owner === undefined ? '' : `ALTER SCHEMA ${ident(old.name)} OWNER TO ${ident(owner)};`
    const sql = `BEGIN; ${ownerSql} ${nameSql} COMMIT;`
    await this.query(sql)
    const schema = await this.retrieve({ id })
    return schema
  }

  async del(id: number, { cascade = false } = {}) {
    const schema = await this.retrieve({ id })
    const sql = `DROP SCHEMA ${ident(schema.name)} ${cascade ? 'CASCADE' : 'RESTRICT'};`
    await this.query(sql)
    return schema
  }
}
