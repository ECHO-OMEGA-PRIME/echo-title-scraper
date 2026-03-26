-- Schema for echo-title-scraper
-- Auto-generated from source code (src/index.ts)
-- D1 Database: echo-title-scraper (24eb83d3-9387-4724-b32a-2214d5681b30)
-- Run: npx wrangler d1 execute echo-title-scraper --remote --file=./schema.sql

DROP TABLE IF EXISTS training_examples;
DROP TABLE IF EXISTS chains;
DROP TABLE IF EXISTS documents;
DROP TABLE IF EXISTS scrape_runs;
DROP TABLE IF EXISTS tracts;

CREATE TABLE tracts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  county TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'TX',
  section TEXT,
  block TEXT,
  survey TEXT,
  abstract_number TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  document_count INTEGER DEFAULT 0,
  chain_complete INTEGER DEFAULT 0,
  last_scraped TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(county, section, block, survey)
);

CREATE TABLE documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tract_id INTEGER NOT NULL,
  doc_type TEXT NOT NULL,
  volume TEXT,
  page TEXT,
  date_filed TEXT,
  date_executed TEXT,
  grantor TEXT,
  grantee TEXT,
  legal_description TEXT,
  instrument_number TEXT,
  consideration TEXT,
  acreage REAL,
  notes TEXT,
  raw_data TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (tract_id) REFERENCES tracts(id)
);

CREATE TABLE chains (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tract_id INTEGER NOT NULL,
  chain_type TEXT NOT NULL DEFAULT 'full',
  documents TEXT DEFAULT '[]',
  gaps TEXT DEFAULT '[]',
  score REAL DEFAULT 0,
  notes TEXT,
  validated INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (tract_id) REFERENCES tracts(id)
);

CREATE TABLE training_examples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  chain_id INTEGER NOT NULL,
  tract_id INTEGER NOT NULL,
  system_prompt TEXT NOT NULL,
  user_prompt TEXT NOT NULL,
  assistant_response TEXT NOT NULL,
  format TEXT NOT NULL DEFAULT 'chatml',
  quality_score REAL DEFAULT 0,
  gap_types TEXT DEFAULT '[]',
  exported INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (chain_id) REFERENCES chains(id),
  FOREIGN KEY (tract_id) REFERENCES tracts(id)
);

CREATE TABLE scrape_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,
  county TEXT,
  status TEXT NOT NULL DEFAULT 'running',
  tracts_processed INTEGER DEFAULT 0,
  docs_found INTEGER DEFAULT 0,
  chains_built INTEGER DEFAULT 0,
  examples_generated INTEGER DEFAULT 0,
  started_at TEXT DEFAULT (datetime('now')),
  completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_tracts_county ON tracts(county);
CREATE INDEX IF NOT EXISTS idx_tracts_status ON tracts(status);
CREATE INDEX IF NOT EXISTS idx_docs_tract ON documents(tract_id);
CREATE INDEX IF NOT EXISTS idx_docs_grantor ON documents(grantor);
CREATE INDEX IF NOT EXISTS idx_docs_grantee ON documents(grantee);
CREATE INDEX IF NOT EXISTS idx_docs_instrument ON documents(instrument_number);
CREATE INDEX IF NOT EXISTS idx_chains_tract ON chains(tract_id);
CREATE INDEX IF NOT EXISTS idx_chains_score ON chains(score);
CREATE INDEX IF NOT EXISTS idx_training_chain ON training_examples(chain_id);
CREATE INDEX IF NOT EXISTS idx_training_exported ON training_examples(exported);
CREATE INDEX IF NOT EXISTS idx_runs_status ON scrape_runs(status);
