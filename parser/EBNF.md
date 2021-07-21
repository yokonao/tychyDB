# EBNF(Extended Backs-Naur Form)

query = select
select = "select" column "from" table (where)*
where = column "=" value
value = string | number
column = ident
table = ident
