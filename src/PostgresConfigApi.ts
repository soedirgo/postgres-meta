import { configSql } from './sql'

export default class PostgresConfigApi {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async getAll() {
    const { data } = await this.query(configSql)
    return data
  }
}
