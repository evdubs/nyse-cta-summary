#lang racket/base

(require db
         racket/cmdline
         racket/list
         racket/sequence
         racket/string
         racket/struct
         srfi/19) ; Time Data Types and Procedures

(struct con-entry
  (message-category
   message-type
   network
   sequence-number
   output-time
   symbol
   suffix
   part-identifier
   prior-closing-date
   high-price
   low-price
   last-price
   total-volume
   open-price
   tick
   financial-status
   short-sale)
  #:transparent)

(struct part-entry
  (message-category
   message-type
   network
   sequence-number
   output-time
   symbol
   suffix
   part-identifier
   prior-closing-date
   high-price
   low-price
   last-price
   total-volume
   open-price
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
           [con-eod-lines (filter (λ (line) (and (string-contains? line "Con_EOD")
                                                 (string-contains? line "16:15"))) lines)]
           [con-eod-entries (map (λ (line) (let ([split-line (regexp-split #rx"," line)])
                                             ; If there's a value in the short-sale column, the CSV actually
                                             ; follows it with a comma. This gives us an extra column. So, we
                                             ; just drop it here.
                                             (if (= 18 (length split-line))
                                                 (apply con-entry (take split-line 17))
                                                 (apply con-entry split-line)))) con-eod-lines)]
           [con-eod-hash (apply hash (flatten (map (λ (entry) (list (con-entry-symbol entry)
                                                                    (hash (con-entry-part-identifier entry) entry)))
                                                   con-eod-entries)))]
           [part-eod-lines (filter (λ (line) (and (string-contains? line "Part_EOD")
                                                  (string-contains? line "16:15"))) lines)]
           [part-eod-entries (map (λ (line) (apply part-entry (regexp-split #rx"," line))) part-eod-lines)]
           [part-eod-entries-from-con (filter (λ (entry) (and (hash-has-key? con-eod-hash (part-entry-symbol entry))
                                                              (hash-has-key? (hash-ref con-eod-hash (part-entry-symbol entry))
                                                                             (part-entry-part-identifier entry))))
                                              part-eod-entries)])
      (for-each (λ (entry)
                  (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process the following entry for date "
                                                                              (date->string (file-date) "~1")))
                                               (displayln (struct->list entry))
                                               (displayln e)
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
    else $3::text::numeric
  end,
  case $4
    when '' then NULL
    else $4::text::numeric
  end,
  $5::text::numeric,
  case $6
    when '' then NULL
    else $6::text::bigint
  end
) on conflict (act_symbol, date) do nothing;
"
                                (string-replace (con-entry-symbol entry) "/" ".")
                                (date->string (file-date) "~1")
                                (string-replace (con-entry-high-price entry) "_" "")
                                (string-replace (con-entry-low-price entry) "_" "")
                                (string-replace (con-entry-last-price entry) "_" "")
                                (string-replace (con-entry-total-volume entry) "_" ""))
                    (commit-transaction dbc)
                    (set! insert-success-counter (add1 insert-success-counter)))) con-eod-entries)
      (sequence-for-each (λ (entry)
                           (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process the following entry for date "
                                                                                       (date->string (file-date) "~1")))
                                                        (displayln (struct->list entry))
                                                        (displayln e)
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
      else $3::text::numeric
    end
where
  act_symbol = (select act_symbol from nasdaq.symbol where cqs_symbol = $1 or nasdaq_symbol = $1) and
  date = $2::text::date;
"
                                         (string-replace (part-entry-symbol entry) "/" ".")
                                         (date->string (file-date) "~1")
                                         (string-replace (part-entry-open-price entry) "_" ""))
                             (commit-transaction dbc)
                             (set! update-success-counter (add1 update-success-counter)))) part-eod-entries-from-con))))

(disconnect dbc)

(displayln (string-append "Attempted to insert " (number->string insert-counter) " rows. "
                          (number->string insert-success-counter) " were successful. "
                          (number->string insert-failure-counter) " failed."))

(displayln (string-append "Attempted to update " (number->string update-counter) " rows. "
                          (number->string update-success-counter) " were successful. "
                          (number->string update-failure-counter) " failed."))
