CREATE SCHEMA nyse;

CREATE TABLE nyse.cta_summary
(
    act_symbol text NOT NULL,
    date date NOT NULL,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume bigint,
    CONSTRAINT cta_summary_pkey PRIMARY KEY (date, act_symbol),
    CONSTRAINT cta_summary_act_symbol_pkey FOREIGN KEY (act_symbol)
        REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
