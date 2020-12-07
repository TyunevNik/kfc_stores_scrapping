select *
from stores st
left join stores_menues mn
on st."store.storeId" = mn."store.storeId"
where
	st."store.title.ru" like '%Новосибирск%' and
	mn."name" = 'Завтрак' and
	'08:45:00' between mn."availability.regular.startTimeLocal" and mn."availability.regular.endTimeLocal"