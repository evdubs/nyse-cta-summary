#lang racket

(require net/ftp)
(require racket/cmdline)
(require srfi/19) ; Time Data Types and Procedures

(define start-date (make-parameter (current-date)))

(define end-date (make-parameter (time-utc->date (add-duration (date->time-utc (current-date))
                                                               (make-time time-duration 0 (* 60 60 24))))))

(command-line
 #:program "racket extract.rkt"
 #:once-each
 [("-e" "--end-date") end
                      "Final date for file retrieval. Defaults to today"
                      (end-date (string->date end "~Y-~m-~d"))]
 [("-s" "--start-date") start
                        "Earliest date for file retrieval. Defaults to tomorrow"
                        (start-date (string->date start "~Y-~m-~d"))])

(define nyxdata-ftp (ftp-establish-connection "ftp.nyxdata.com" 21 "anonymous" "anonymous"))
(ftp-cd nyxdata-ftp "cts_summary_files")

(for-each (λ (jd) (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to retrieve CTA file for " (date->string (julian-day->date jd) "~1")))
                                               (displayln ((error-value->string-handler) e 1000)))])
                    (ftp-download-file nyxdata-ftp "/var/tmp/nyse/cta-summary"
                                       (string-append "CTA.Summary.EODSUM." (date->string (julian-day->date jd) "~Y~m~d") ".csv"))))
          (range (date->julian-day (start-date)) (date->julian-day (end-date))))

(ftp-close-connection nyxdata-ftp)
