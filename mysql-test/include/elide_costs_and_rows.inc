# Usage: --replace_regex $elide_costs_and_rows

#  (cost=1.26..2.52 rows=2)  -> (...)
#  (cost=0.35 rows=1)        -> (...)
#  (cost=0.35, row=1)        -> (...)
let $elide_costs_and_rows = /[(]cost=[0-9.e+-]+[,]? row[s]?=[0-9.e+-]+[)]/(...)/;
