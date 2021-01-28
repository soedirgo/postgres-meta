import { ident, literal } from 'pg-format'
import { rolesSql } from './sql'

export default class PostgresMetaRole {
  query: Function

  constructor(query: Function) {
    this.query = query
  }

  async list() {
    const { data } = await this.query(rolesSql)
    return data
  }

  async retrieve({ id }: { id: number }): Promise<any>
  async retrieve({ name }: { name: string }): Promise<any>
  async retrieve({ id, name }: { id?: number; name?: string }) {
    if (id) {
      const sql = `${rolesSql} WHERE oid = ${literal(id)};`
      const {
        data: [role],
      } = await this.query(sql)
      return role
    } else if (name) {
      const sql = `${rolesSql} WHERE rolname = ${literal(name)};`
      const {
        data: [role],
      } = await this.query(sql)
      return role
    } else {
      // TODO error
    }
  }

  async create({
    name,
    is_superuser = false,
    can_create_db = false,
    can_create_role = false,
    inherit_role = true,
    can_login = false,
    is_replication_role = false,
    can_bypass_rls = false,
    connection_limit = -1,
    password,
    valid_until,
    member_of,
    members,
    admins,
  }: {
    name: string
    is_superuser?: boolean
    can_create_db?: boolean
    can_create_role?: boolean
    inherit_role?: boolean
    can_login?: boolean
    is_replication_role?: boolean
    can_bypass_rls?: boolean
    connection_limit?: number
    password?: string
    valid_until?: string
    member_of?: string[]
    members?: string[]
    admins?: string[]
  }) {
    const isSuperuserClause = is_superuser ? 'SUPERUSER' : 'NOSUPERUSER'
    const canCreateDbClause = can_create_db ? 'CREATEDB' : 'NOCREATEDB'
    const canCreateRoleClause = can_create_role ? 'CREATEROLE' : 'NOCREATEROLE'
    const inheritRoleClause = inherit_role ? 'INHERIT' : 'NOINHERIT'
    const canLoginClause = can_login ? 'LOGIN' : 'NOLOGIN'
    const isReplicationRoleClause = is_replication_role ? 'REPLICATION' : 'NOREPLICATION'
    const canBypassRlsClause = can_bypass_rls ? 'BYPASSRLS' : 'NOBYPASSRLS'
    const connectionLimitClause = `CONNECTION LIMIT ${connection_limit}`
    const passwordClause = password === undefined ? '' : `PASSWORD ${literal(password)}`
    const validUntilClause = valid_until === undefined ? '' : `VALID UNTIL ${literal(valid_until)}`
    const memberOfClause = member_of === undefined ? '' : `IN ROLE ${member_of.join(',')}`
    const membersClause = members === undefined ? '' : `ROLE ${members.join(',')}`
    const adminsClause = admins === undefined ? '' : `ADMIN ${admins.join(',')}`

    const sql = `
CREATE ROLE ${ident(name)}
WITH
  ${isSuperuserClause}
  ${canCreateDbClause}
  ${canCreateRoleClause}
  ${inheritRoleClause}
  ${canLoginClause}
  ${isReplicationRoleClause}
  ${canBypassRlsClause}
  ${connectionLimitClause}
  ${passwordClause}
  ${validUntilClause}
  ${memberOfClause}
  ${membersClause}
  ${adminsClause};`
    await this.query(sql)
    const role = await this.retrieve({ name })
    return role
  }

  async update(
    id: number,
    {
      name,
      is_superuser,
      can_create_db,
      can_create_role,
      inherit_role,
      can_login,
      is_replication_role,
      can_bypass_rls,
      connection_limit,
      password,
      valid_until,
    }: {
      name?: string
      is_superuser?: boolean
      can_create_db?: boolean
      can_create_role?: boolean
      inherit_role?: boolean
      can_login?: boolean
      is_replication_role?: boolean
      can_bypass_rls?: boolean
      connection_limit?: number
      password?: string
      valid_until?: string
    }
  ) {
    const old = await this.retrieve({ id })

    const nameSql =
      name === undefined ? '' : `ALTER ROLE ${ident(old.name)} RENAME TO ${ident(name)};`
    let isSuperuserClause = ''
    if (is_superuser !== undefined) {
      isSuperuserClause = is_superuser ? 'SUPERUSER' : 'NOSUPERUSER'
    }
    let canCreateDbClause = ''
    if (can_create_db !== undefined) {
      canCreateDbClause = can_create_db ? 'CREATEDB' : 'NOCREATEDB'
    }
    let canCreateRoleClause = ''
    if (can_create_role !== undefined) {
      canCreateRoleClause = can_create_role ? 'CREATEROLE' : 'NOCREATEROLE'
    }
    let inheritRoleClause = ''
    if (inherit_role !== undefined) {
      inheritRoleClause = inherit_role ? 'INHERIT' : 'NOINHERIT'
    }
    let canLoginClause = ''
    if (can_login !== undefined) {
      canLoginClause = can_login ? 'LOGIN' : 'NOLOGIN'
    }
    let isReplicationRoleClause = ''
    if (is_replication_role !== undefined) {
      isReplicationRoleClause = is_replication_role ? 'REPLICATION' : 'NOREPLICATION'
    }
    let canBypassRlsClause = ''
    if (can_bypass_rls !== undefined) {
      canBypassRlsClause = can_bypass_rls ? 'BYPASSRLS' : 'NOBYPASSRLS'
    }
    const connectionLimitClause =
      connection_limit === undefined ? '' : `CONNECTION LIMIT ${connection_limit}`
    let passwordClause = password === undefined ? '' : `PASSWORD ${literal(password)}`
    let validUntilClause = valid_until === undefined ? '' : `VALID UNTIL ${literal(valid_until)}`

    // nameSql must be last
    const sql = `
BEGIN;
  ALTER ROLE ${ident(old.name)}
    ${isSuperuserClause}
    ${canCreateDbClause}
    ${canCreateRoleClause}
    ${inheritRoleClause}
    ${canLoginClause}
    ${isReplicationRoleClause}
    ${canBypassRlsClause}
    ${connectionLimitClause}
    ${passwordClause}
    ${validUntilClause};
  ${nameSql}
COMMIT;`
    await this.query(sql)
    const role = await this.retrieve({ id })
    return role
  }

  async del(id: number) {
    const role = await this.retrieve({ id })
    const sql = `DROP ROLE ${ident(role.name)};`
    await this.query(sql)
    return role
  }
}
