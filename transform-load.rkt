#lang racket

(require db)
(require racket/struct)
(require srfi/19) ; Time Data Types and Procedures

(display (string-append "CTA.Summary.EODSUM file date [" (date->string (current-date) "~1") "]: "))
(flush-output)
(define file-date
  (let ([date-string-input (read-line)])
    (if (equal? "" date-string-input) (current-date)
        (string->date date-string-input "~Y-~m-~d"))))

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

(display (string-append "db user [user]: "))
(flush-output)
(define db-user
  (let ([db-user-input (read-line)])
    (if (equal? "" db-user-input) "user"
        db-user-input)))

(display (string-append "db name [local]: "))
(flush-output)
(define db-name
  (let ([db-name-input (read-line)])
    (if (equal? "" db-name-input) "local"
        db-name-input)))

(display (string-append "db pass []: "))
(flush-output)
(define db-pass (read-line))

(define dbc (postgresql-connect #:user db-user #:database db-name #:password db-pass))

(with-input-from-file
    (string-append "/var/tmp/nyse/cta-summary/CTA.Summary.EODSUM."
                   (date->string file-date "~Y~m~d")
                   ".csv")
  (λ ()
    (let* ([lines (sequence->list (in-lines))]
           [con-eod-lines (filter (λ (line) (and (string-contains? line "Con_EOD")
                                                 (string-contains? line "16:15"))) lines)]
           [con-eod-entries (map (λ (line) (apply con-entry (regexp-split #rx"," line))) con-eod-lines)]
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
                                                                              (date->string file-date "~1")))
                                               (displayln (struct->list entry))
                                               (displayln ((error-value->string-handler) e 1000))
                                               (rollback-transaction dbc))])
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
                                (date->string file-date "~1")
                                (string-replace (con-entry-high-price entry) "_" "")
                                (string-replace (con-entry-low-price entry) "_" "")
                                (string-replace (con-entry-last-price entry) "_" "")
                                (string-replace (con-entry-total-volume entry) "_" ""))
                    (commit-transaction dbc))) con-eod-entries)
      (sequence-for-each (λ (entry)
                           (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process the following entry for date "
                                                                                       (date->string file-date "~1")))
                                                        (displayln (struct->list entry))
                                                        (displayln ((error-value->string-handler) e 1000))
                                                        (rollback-transaction dbc))])
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
                                         (date->string file-date "~1")
                                         (string-replace (part-entry-open-price entry) "_" ""))
                             (commit-transaction dbc))) part-eod-entries-from-con))))

(disconnect dbc)