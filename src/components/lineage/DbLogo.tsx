import { Database } from 'lucide-react'
import type { ComponentProps } from 'react'
import type { DatabaseType } from './types'

import postgresqlLogo from '../../assets/db/postgresql.svg'
import snowflakeLogo from '../../assets/db/snowflake.svg'
import mysqlLogo from '../../assets/db/mysql.svg'
import oracleLogo from '../../assets/db/oracle.svg'
import sqlserverLogo from '../../assets/db/sqlserver.svg'
import databricksLogo from '../../assets/Databricks.jpeg'

const LOGOS: Record<Exclude<DatabaseType, 'unknown'>, string> = {
  postgresql: postgresqlLogo,
  snowflake: snowflakeLogo,
  mysql: mysqlLogo,
  oracle: oracleLogo,
  sqlserver: sqlserverLogo,
  databricks: databricksLogo
}

export default function DbLogo({
  databaseType,
  className,
  ...rest
}: { databaseType: DatabaseType } & Omit<ComponentProps<'img'>, 'src' | 'alt'>) {
  if (databaseType === 'unknown' || !LOGOS[databaseType]) {
    return <Database className={className || 'w-5 h-5 text-slate-700'} aria-hidden="true" />
  }

  return (
    <img
      src={LOGOS[databaseType]}
      alt=""
      className={className || 'w-5 h-5 object-contain filter grayscale brightness-0'}
      {...rest}
    />
  )
}
