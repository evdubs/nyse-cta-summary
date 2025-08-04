# nyse-cta-summary

These Racket programs will download the NYSE CTA Summary EODSUM CSV files and insert the aggregate price data into a PostgreSQL database. 
The intended usage is:

```bash
$ racket extract.rkt
$ racket transform-load.2019-03-28.rkt
```

You will need to provide a database password for `transform-load.2019-03-28.rkt`. The available parameters are:

```bash
$ racket extract.rkt -h
racket extract.rkt [ <option> ... ]
 where <option> is one of
  -e <end>, --end-date <end> : Final date for file retrieval. Defaults to tomorrow
  -s <start>, --start-date <start> : Earliest date for file retrieval. Defaults to today
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-`. For
  example: `-h-` is the same as `-h --`

$ racket transform-load.2019-03-28.rkt -h
racket transform-load.2019-03-28.rkt [ <option> ... ]
 where <option> is one of
  -d <date>, --file-date <date> : NYSE CTA Summary File file date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -t <time>, --target-time <time> : Target Time. Used to extract data based on the TransTime field. Defaults to 16:15
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-`. For
  example: `-h-` is the same as `-h --`
```

The provided `schema.sql` file shows the expected schema within the target PostgreSQL instance. 
This process assumes you can write to a `/var/local/nyse/cta-summary` folder. This process also assumes you have loaded your database with 
NASDAQ symbol file information. This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project.

### Dependencies

It is recommended that you start with the standard Racket distribution. With that, you will need to install the following packages:

```bash
$ raco pkg install --skip-installed gregor
```

## Format update (2019-03-28)

On 2019-03-28, the CTA Summary EODSUM CSV format changed. The new transform-load script to run is called `transform-load.2019-03-28.rkt`. 
No schema changes are necessary.
