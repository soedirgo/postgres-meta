import { types, Client, Pool } from 'pg'
types.setTypeParser(20, parseInt)

export const init = (connectionString: string, { pooled = true } = {}) => {
  const client = pooled ? new Pool({ connectionString }) : new Client({ connectionString })
  return async (sql: string) => {
    const { rows } = await client.query(sql)
    return { data: rows, error: null }
  }
}
