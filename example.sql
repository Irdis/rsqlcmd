select 1, 'hello' as Title
union all
select 2, 'abc'
union all
select 3, 'abc'
union all
select 4, 'abc'
union all
select 5, 'abc'
union all
select 6, 'abc'
union all
select 7, 'abc'
union all
select 8, 'abc'
union all
select NULL, 'abc'

select 1 as Id, cast('Ivan' as varchar(40)) as Name, cast('Rosa' as varchar(max)) as Cat, cast(49.001 as decimal(21, 7)) as Size
