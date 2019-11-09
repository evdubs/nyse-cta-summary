#lang racket/base

(require gregor
         gregor/period
         net/ftp
         racket/cmdline
         racket/list)

(define start-date (make-parameter (today)))

(define end-date (make-parameter (+days (today) 1)))

(command-line
 #:program "racket extract.rkt"
 #:once-each
 [("-e" "--end-date") end
                      "Final date for file retrieval. Defaults to tomorrow"
                      (end-date (iso8601->date end))]
 [("-s" "--start-date") start
                        "Earliest date for file retrieval. Defaults to today"
                        (start-date (iso8601->date start))])

(define nyxdata-ftp (ftp-establish-connection "ftp.nyxdata.com" 21 "anonymous" "anonymous"))
(ftp-cd nyxdata-ftp "cts_summary_files")

(for-each (λ (i) (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to retrieve CTA file for " (~t (+days (start-date) i) "yyyy-MM-dd")))
                                               (displayln ((error-value->string-handler) e 1000)))])
                    (ftp-download-file nyxdata-ftp "/var/tmp/nyse/cta-summary"
                                       (string-append "CTA.Summary.EODSUM." (~t (+days (start-date) i) "yyyyMMdd") ".csv"))))
          (range 0 (period-ref (period-between (start-date) (end-date) '(days)) 'days)))

(ftp-close-connection nyxdata-ftp)
