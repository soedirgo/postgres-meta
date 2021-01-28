import { versionSql } from './sql'

export default class PostgresMetaVersion {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async retrieve() {
    const {
      data: [version],
    } = await this.query(versionSql)
    return version
  }
}
