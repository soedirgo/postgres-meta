import { typesSql } from './sql'

export default class PostgresMetaType {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async list() {
    const { data } = await this.query(typesSql)
    return data
  }
}
