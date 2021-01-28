import { ident, literal } from 'pg-format'
import { publicationsSql } from './sql'

export default class PostgresMetaPublication {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async list() {
    const { data } = await this.query(publicationsSql)
    return data
  }

  async retrieve({ id }: { id: number }): Promise<any>
  async retrieve({ name }: { name: string }): Promise<any>
  async retrieve({ id, name }: { id?: number; name?: string }) {
    if (id) {
      const sql = `${publicationsSql} WHERE p.oid = ${literal(id)};`
      const {
        data: [publication],
      } = await this.query(sql)
      return publication
    } else if (name) {
      const sql = `${publicationsSql} WHERE p.pubname = ${literal(name)};`
      const {
        data: [publication],
      } = await this.query(sql)
      return publication
    } else {
      // TODO error
    }
  }

  async create({
    name,
    publish_insert = false,
    publish_update = false,
    publish_delete = false,
    publish_truncate = false,
    tables,
  }: {
    name: string
    publish_insert?: boolean
    publish_update?: boolean
    publish_delete?: boolean
    publish_truncate?: boolean
    tables?: string[]
  }) {
    let tableClause: string
    if (tables === undefined) {
      tableClause = 'FOR ALL TABLES'
    } else if (tables.length === 0) {
      tableClause = ''
    } else {
      tableClause = `FOR TABLE ${tables.map(ident).join(',')}`
    }

    let publishOps = []
    if (publish_insert) publishOps.push('insert')
    if (publish_update) publishOps.push('update')
    if (publish_delete) publishOps.push('delete')
    if (publish_truncate) publishOps.push('truncate')

    const sql = `
CREATE PUBLICATION ${ident(name)} ${tableClause}
  WITH (publish = '${publishOps.join(',')}');`
    await this.query(sql)
    const publication = await this.retrieve({ name })
    return publication
  }

  async update(
    id: number,
    {
      name,
      owner,
      publish_insert,
      publish_update,
      publish_delete,
      publish_truncate,
      tables,
    }: {
      name?: string
      owner?: string
      publish_insert?: boolean
      publish_update?: boolean
      publish_delete?: boolean
      publish_truncate?: boolean
      tables?: string[]
    }
  ) {
    const old = await this.retrieve({ id })

    // Need to work around the limitations of the SQL. Can't add/drop tables from
    // a publication with FOR ALL TABLES. Can't use the SET TABLE clause without
    // at least one table.
    //
    //                              new tables
    //
    //                      | undefined |    string[]     |
    //             ---------|-----------|-----------------|
    //                 null |    ''     | 400 Bad Request |
    // old tables  ---------|-----------|-----------------|
    //             string[] |    ''     |    See below    |
    //
    //                              new tables
    //
    //                      |    []     |      [...]      |
    //             ---------|-----------|-----------------|
    //                   [] |    ''     |    SET TABLE    |
    // old tables  ---------|-----------|-----------------|
    //                [...] | DROP all  |    SET TABLE    |
    //
    let tableSql: string
    if (tables === undefined) {
      tableSql = ''
    } else if (old.tables === null) {
      throw new Error('Tables cannot be added to or dropped from FOR ALL TABLES publications')
    } else if (tables.length > 0) {
      tableSql = `ALTER PUBLICATION ${ident(old.name)} SET TABLE ${tables.map(ident).join(',')};`
    } else if (old.tables.length === 0) {
      tableSql = ''
    } else {
      tableSql = `ALTER PUBLICATION ${ident(old.name)} DROP TABLE ${old.tables
        .map(ident)
        .join(',')};`
    }

    let publishOps = []
    if (publish_insert ?? old.publish_insert) publishOps.push('insert')
    if (publish_update ?? old.publish_update) publishOps.push('update')
    if (publish_delete ?? old.publish_delete) publishOps.push('delete')
    if (publish_truncate ?? old.publish_truncate) publishOps.push('truncate')
    const publishSql = `ALTER PUBLICATION ${ident(old.name)} SET (publish = '${publishOps.join(
      ','
    )}');`

    const ownerSql =
      owner === undefined ? '' : `ALTER PUBLICATION ${ident(old.name)} OWNER TO ${ident(owner)};`

    const nameSql =
      name === undefined || name === old.name
        ? ''
        : `ALTER PUBLICATION ${ident(old.name)} RENAME TO ${ident(name)};`

    // nameSql must be last
    const sql = `BEGIN; ${tableSql} ${publishSql} ${ownerSql} ${nameSql} COMMIT;`
    await this.query(sql)
    const publication = await this.retrieve({ id })
    return publication
  }

  async del(id: number) {
    const publication = await this.retrieve({ id })
    const sql = `DROP PUBLICATION IF EXISTS ${ident(publication.name)};`
    await this.query(sql)
    return publication
  }
}
