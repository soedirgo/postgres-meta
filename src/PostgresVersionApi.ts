import { versionSql } from './sql'

export default class PostgresVersionApi {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async get() {
    const {
      data: [version],
    } = await this.query(versionSql)
    return version
  }
}
