import { typesSql } from './sql'

export default class PostgresTypeApi {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async getAll() {
    const { data } = await this.query(typesSql)
    return data
  }
}
