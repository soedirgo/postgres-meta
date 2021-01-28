import { configSql } from './sql'

export default class PostgresMetaConfig {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async list() {
    const { data } = await this.query(configSql)
    return data
  }
}
