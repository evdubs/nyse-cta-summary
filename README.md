# nyse-cta-summary

These Racket programs will download the NYSE CTA Summary EODSUM CSV files and insert the aggregate price data into a PostgreSQL database. 
The intended usage is:

```bash
$ racket extract.rkt
$ racket transform-load.rkt
```

The provided schema.sql file shows the expected schema within the target PostgreSQL instance. 
This process assumes you can write to a /var/tmp/nyse/cta-summary folder. This process also assumes you have loaded your database with 
NASDAQ symbol file information. This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project.

## Format update (2019-03-28)

On 2019-03-28, the CTA Summary EODSUM CSV format changed. The new transform-load script to run is called `transform-load.2019-03-28.rkt`. 
No schema changes are necessary.
