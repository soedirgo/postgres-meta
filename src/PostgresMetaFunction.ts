import { literal } from 'pg-format'
import { DEFAULT_SYSTEM_SCHEMAS } from './constants'
import { functionsSql } from './sql'

export default class PostgresMetaFunction {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async list({ includeSystemSchemas = false } = {}) {
    const sql = includeSystemSchemas
      ? functionsSql
      : `${functionsSql} WHERE NOT (n.nspname IN (${DEFAULT_SYSTEM_SCHEMAS.map(literal).join(
          ','
        )}));`
    const { data } = await this.query(sql)
    return data
  }
}
