-- Add users to group

 SELECT
	'alter group ' + LOWER(TRIM(groname)) + ' add user "' + LOWER(TRIM(usename)) + '";'
FROM
	pg_user,
	pg_group
WHERE
	pg_user.usesysid = ANY(pg_group.grolist)
	AND pg_group.groname in (
	SELECT
		DISTINCT pg_group.groname
	from
		pg_group);
