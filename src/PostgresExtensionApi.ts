import { ident, literal } from 'pg-format'
import { extensionsSql } from './sql'

export default class PostgresExtensionApi {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async getAll() {
    const { data } = await this.query(extensionsSql)
    return data
  }

  async getByName(name: string) {
    const sql = `${extensionsSql} WHERE name = ${name};`
    const {
      data: [extension],
    } = await this.query(sql)
    return extension
  }

  async create({
    name,
    schema,
    version,
    cascade = false,
  }: {
    name: string
    schema?: string
    version?: string
    cascade?: boolean
  }) {
    const sql = `
CREATE EXTENSION ${ident(name)}
  ${schema === undefined ? '' : `SCHEMA ${ident(schema)}`}
  ${version === undefined ? '' : `VERSION ${literal(version)}`}
  ${cascade ? 'CASCADE' : ''};`
    await this.query(sql)
    const extension = await this.getByName(name)
    return extension
  }

  async alter(
    name: string,
    {
      update = false,
      version,
      schema,
    }: {
      update?: boolean
      version?: string
      schema?: string
    }
  ) {
    let updateSql = ''
    if (update) {
      updateSql = `ALTER EXTENSION ${ident(name)} UPDATE ${
        version === undefined ? '' : `TO ${literal(version)}`
      };`
    }
    const schemaSql =
      schema === undefined ? '' : `ALTER EXTENSION ${ident(name)} SET SCHEMA ${ident(schema)};`

    const sql = `BEGIN; ${updateSql} ${schemaSql} COMMIT;`
    await this.query(sql)
    const extension = await this.getByName(name)
    return extension
  }

  async drop(name: string, { cascade = false } = {}) {
    const extension = await this.getByName(name)
    const sql = `DROP EXTENSION ${ident(name)} ${cascade ? 'CASCADE' : 'RESTRICT'};`
    await this.query(sql)
    return extension
  }
}
