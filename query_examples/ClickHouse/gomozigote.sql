SELECT count(*) from all_chr21
WHERE arrayAll(x -> x.calls[1] = x.calls[2], genotypes);