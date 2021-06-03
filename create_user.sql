
select
	'CREATE USER "' || LOWER(TRIM(usename))|| '" with PASSWORD ' || CHR(39)|| 'MyP@ss1@#$' || CHR(39)||
	case
		when usecreatedb = 't' then ' CREATEDB'
		else ' NOCREATEDB'
	end ||
	case
		when usesuper = 't' then ' CREATEUSER'
		else ' NOCREATEUSER'
	end ||' connection limit '||trim(useconnlimit) ||';'
from
pg_user_info
where
usesysid >= 100 ;
