
select
	'create group ' + trim(groname) + ' ;'
from
	pg_group;

create group wlm_etl ;
create group wlm_highest ;
create group wlm_high ;
create group wlm_unica ;
create group mktg_admins ;
create group mktg_admin_group ;
create group mod_readwrite_group ;
create group mktg_platform ;
create group mktg_analytics ;
create group mktg_ops ;
create group mad_readwrite_group ;
create group mpl_readwrite_group ;
create group mktg_readonly_group ;
