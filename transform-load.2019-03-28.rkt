#lang racket/base

(require db
         racket/cmdline
         racket/list
         racket/sequence
         racket/string
         racket/struct
         srfi/19) ; Time Data Types and Procedures

(struct row-entry
  (message-category-type
   transaction-time
   sequence-number
   symbol
   instrument-type
   part-identifier
   previous-close-price-date
   last-close-price
   high-price
   low-price
   open-price
   total-volume
   short-sale-restriction
   primary-listing-market
   financial-status
   number-of-participants
   tick)
  #:transparent)

(define file-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket transform-load.rkt"
 #:once-each
 [("-d" "--file-date") date
                       "NYSE CTA Summary File file date. Defaults to today"
                       (file-date (string->date date "~Y-~m-~d"))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define insert-counter 0)
(define insert-success-counter 0)
(define insert-failure-counter 0)

(define update-counter 0)
(define update-success-counter 0)
(define update-failure-counter 0)

(with-input-from-file
    (string-append "/var/tmp/nyse/cta-summary/CTA.Summary.EODSUM."
                   (date->string (file-date) "~Y~m~d")
                   ".csv")
  (λ ()
    (let* ([lines (sequence->list (in-lines))]
           [con-eod-lines (filter (λ (line) (and (string-contains? line "ConsEOD")
                                                 (string-contains? line "16:15"))) lines)]
           [con-eod-entries (map (λ (line) (let ([split-line (regexp-split #rx"," line)])
                                             (apply row-entry split-line))) con-eod-lines)]
           [con-eod-hash (apply hash (flatten (map (λ (entry) (list (row-entry-symbol entry)
                                                                    (hash (row-entry-part-identifier entry) entry)))
                                                   con-eod-entries)))]
           [part-eod-lines (filter (λ (line) (and (string-contains? line "PartEOD")
                                                  (string-contains? line "16:15"))) lines)]
           [part-eod-entries (map (λ (line) (apply row-entry (regexp-split #rx"," line))) part-eod-lines)]
           [part-eod-entries-from-con (filter (λ (entry) (and (hash-has-key? con-eod-hash (row-entry-symbol entry))
                                                              (hash-has-key? (hash-ref con-eod-hash (row-entry-symbol entry))
                                                                             (row-entry-part-identifier entry))))
                                              part-eod-entries)])
      (for-each (λ (entry)
                  (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process the following entry for date "
                                                                              (date->string (file-date) "~1")))
                                               (displayln (struct->list entry))
                                               (displayln ((error-value->string-handler) e 1000))
                                               (rollback-transaction dbc)
                                               (set! insert-failure-counter (add1 insert-failure-counter)))])
                    (set! insert-counter (add1 insert-counter))
                    (start-transaction dbc)
                    (query-exec dbc "
insert into nyse.cta_summary (
  act_symbol,
  date,
  high,
  low,
  close,
  volume
) values (
  (select act_symbol from nasdaq.symbol where cqs_symbol = $1 or nasdaq_symbol = $1),
  $2::text::date,
  case $3
    when '' then NULL
    else trim(trailing '0' from $3::text)::numeric
  end,
  case $4
    when '' then NULL
    else trim(trailing '0' from $4::text)::numeric
  end,
  trim(trailing '0' from $5::text)::numeric,
  case $6
    when '' then NULL
    else $6::text::bigint
  end
) on conflict (act_symbol, date) do nothing;
"
                                (string-replace (string-trim (row-entry-symbol entry)) "/" ".")
                                (date->string (file-date) "~1")
                                (string-replace (row-entry-high-price entry) "_" "")
                                (string-replace (row-entry-low-price entry) "_" "")
                                (string-replace (row-entry-last-close-price entry) "_" "")
                                (string-replace (row-entry-total-volume entry) "_" ""))
                    (commit-transaction dbc)
                    (set! insert-success-counter (add1 insert-success-counter)))) con-eod-entries)
      (sequence-for-each (λ (entry)
                           (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process the following entry for date "
                                                                                       (date->string (file-date) "~1")))
                                                        (displayln (struct->list entry))
                                                        (displayln ((error-value->string-handler) e 1000))
                                                        (rollback-transaction dbc)
                                                        (set! update-failure-counter (add1 update-failure-counter)))])
                             (set! update-counter (add1 update-counter))
                             (start-transaction dbc)
                             (query-exec dbc "
update
  nyse.cta_summary
set
  open =
    case $3
      when '' then NULL
      else trim(trailing '0' from $3::text)::numeric
    end
where
  act_symbol = (select act_symbol from nasdaq.symbol where cqs_symbol = $1 or nasdaq_symbol = $1) and
  date = $2::text::date;
"
                                         (string-replace (string-trim (row-entry-symbol entry)) "/" ".")
                                         (date->string (file-date) "~1")
                                         (string-replace (row-entry-open-price entry) "_" ""))
                             (commit-transaction dbc)
                             (set! update-success-counter (add1 update-success-counter)))) part-eod-entries-from-con))))

(disconnect dbc)

(displayln (string-append "Attempted to insert " (number->string insert-counter) " rows. "
                          (number->string insert-success-counter) " were successful. "
                          (number->string insert-failure-counter) " failed."))

(displayln (string-append "Attempted to update " (number->string update-counter) " rows. "
                          (number->string update-success-counter) " were successful. "
                          (number->string update-failure-counter) " failed."))
