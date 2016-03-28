-- TODO: replace path with large data set path
bigrams = LOAD '$INPUT' AS (bigram:chararray, year:int, count:double, books:double);

-- Group the data by bigram.
grouped = GROUP bigrams BY bigram;

-- Determine the average number of appearances per book.
averaged = FOREACH grouped GENERATE group, (SUM(bigrams.count) / SUM(bigrams.books)) AS avg:double;

-- Order averaged by the average value descending, and the group ascending.
ordered = ORDER averaged BY avg DESC, group ASC;

-- Take the top ten results.
top_ten = LIMIT ordered 10;

-- Store results on S3.
STORE top_ten INTO '$OUTPUT/run1';