import { init } from './db'
import PostgresColumnApi from './PostgresColumnApi'
import PostgresConfigApi from './PostgresConfigApi'
import PostgresExtensionApi from './PostgresExtensionApi'
import PostgresFunctionApi from './PostgresFunctionApi'
import PostgresPolicyApi from './PostgresPolicyApi'
import PostgresPublicationApi from './PostgresPublicationApi'
import PostgresRoleApi from './PostgresRoleApi'
import PostgresSchemaApi from './PostgresSchemaApi'
import PostgresTableApi from './PostgresTableApi'
import PostgresTypeApi from './PostgresTypeApi'
import PostgresVersionApi from './PostgresVersionApi'

export default class PostgresApi {
  query: Function
  column: PostgresColumnApi
  config: PostgresConfigApi
  extension: PostgresExtensionApi
  function: PostgresFunctionApi
  policy: PostgresPolicyApi
  publication: PostgresPublicationApi
  role: PostgresRoleApi
  schema: PostgresSchemaApi
  table: PostgresTableApi
  type: PostgresTypeApi
  version: PostgresVersionApi

  constructor(connectionString: string) {
    this.query = init(connectionString)
    this.column = new PostgresColumnApi(this.query)
    this.config = new PostgresConfigApi(this.query)
    this.extension = new PostgresExtensionApi(this.query)
    this.function = new PostgresFunctionApi(this.query)
    this.policy = new PostgresPolicyApi(this.query)
    this.publication = new PostgresPublicationApi(this.query)
    this.role = new PostgresRoleApi(this.query)
    this.schema = new PostgresSchemaApi(this.query)
    this.table = new PostgresTableApi(this.query)
    this.type = new PostgresTypeApi(this.query)
    this.version = new PostgresVersionApi(this.query)
  }
}
