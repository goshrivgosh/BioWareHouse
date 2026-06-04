SELECT start, end, splitByChar(',', coalesce(attributes['AC'], ''))
FROM all_chr21 WHERE (toFloat64OrNull(arrayElement(splitByChar(',', coalesce(attributes['AC'], '')), 1)) > 10)
order by start, end;