#total stock per cik (A)

SELECT cik, COUNT(cusip)
FROM raw_13f_data
WHERE put_call = NULL
GROUP BY cik
ORDER BY cik DESC;

#total unique stock per cik (B)

SELECT cik, COUNT(DISTINCT cusip)
FROM raw_13f_data
WHERE put_call = NULL
GROUP BY cik
ORDER BY cik DESC;

# double x = B/A then compare them amount hedge funds higher percentage means high stock turnover rate
