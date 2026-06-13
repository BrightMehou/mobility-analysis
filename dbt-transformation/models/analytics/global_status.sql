select
	status,
	count(*) as nb
from
	{{ ref('station') }}
group by status