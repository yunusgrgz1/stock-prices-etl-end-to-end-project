-- Sector table
CREATE TABLE sectors (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Industry table
CREATE TABLE industries (
    id SERIAL PRIMARY KEY,
    sector_id INTEGER REFERENCES sectors(id),
    name TEXT NOT NULL,
    UNIQUE (sector_id, name)
);

-- Exchange table
CREATE TABLE exchanges (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    code TEXT UNIQUE
);

-- Companies table
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    sector_id INTEGER REFERENCES sectors(id),
    industry_id INTEGER REFERENCES industries(id),
    exchange_id INTEGER REFERENCES exchanges(id),
    country TEXT,
    founded_year INTEGER
);

-- Prices table
CREATE TABLE prices (
    company_id INTEGER REFERENCES companies(id),
    date DATE NOT NULL,
    open NUMERIC(10,2),
    high NUMERIC(10,2),
    low NUMERIC(10,2),
    close NUMERIC(10,2),
    volume BIGINT,
    PRIMARY KEY (company_id, date)
);
