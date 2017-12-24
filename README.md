# nyse-cta-summary

These Racket programs will download the NYSE CTA Summary EODSUM CSV files and insert the aggregate price data into a PostgreSQL database. 
The intended usage is:

```bash
$ Racket extract.rkt
$ Racket transform-load.rkt
```

The provided schema.sql file shows the expected schema within the target PostgreSQL instance. 
This process assumes you can write to a /var/tmp/nyse/cta-summary folder. This process also assumes you have loaded your database with 
NASDAQ symbol file information. This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project.
