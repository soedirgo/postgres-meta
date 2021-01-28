import { functionsSql } from './sql'

export default class PostgresMetaFunction {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async list() {
    const { data } = await this.query(functionsSql)
    return data
  }
}
