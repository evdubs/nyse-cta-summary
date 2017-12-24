#lang racket

(require net/ftp)
(require srfi/19) ;; Time Data Types and Procedures

(display (string-append "start date [" (date->string (current-date) "~1") "]: "))
(flush-output)
(define start-date
  (let ([date-string-input (read-line)])
    (if (equal? "" date-string-input) (current-date)
        (string->date date-string-input "~Y-~m-~d"))))

(display (string-append "end date [" (date->string (current-date) "~1") "]: "))
(flush-output)
(define end-date
  (let ([date-string-input (read-line)])
    (if (equal? "" date-string-input) (current-date)
        (string->date date-string-input "~Y-~m-~d"))))

(define nyxdata-ftp (ftp-establish-connection "ftp.nyxdata.com" 21 "anonymous" "anonymous"))
(ftp-cd nyxdata-ftp "cts_summary_files")

(for-each (λ (jd) (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to retrieve CTA file for " (date->string (julian-day->date jd) "~1")))
                                               (displayln ((error-value->string-handler) e 1000)))])
                    (ftp-download-file nyxdata-ftp "/var/tmp/nyse/cta-summary"
                                       (string-append "CTA.Summary.EODSUM." (date->string (julian-day->date jd) "~Y~m~d") ".csv"))))
            (range (date->julian-day start-date) (date->julian-day end-date)))

(ftp-close-connection nyxdata-ftp)
