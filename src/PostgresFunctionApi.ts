import { functionsSql } from './sql'

export default class PostgresFunctionApi {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async getAll() {
    const { data } = await this.query(functionsSql)
    return data
  }
}
