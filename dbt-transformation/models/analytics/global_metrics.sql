select
	count(*) as nb_station,
	sum(capacity) as capacity,
	sum(bicycle_docks_available) as bicycle_docks_available,
	sum(bicycle_available) as bicycle_available
from
	{{ ref('station') }}