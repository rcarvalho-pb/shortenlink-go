package storage

import (
	"database/sql"
	"time"
)

type URL struct {
	ID        int
	Original  string
	Short     string
	CreatedAt time.Time
	ExpiresAt time.Time
	Hits      int
}

type Store struct {
	db *sql.DB
}

func NewStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	if _, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS urls(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		original TEXT NOT NULL UNIQUE,
		short TEXT NOT NULL UNIQUE,
		created_at DATETIME NOT NULL DEFAULT DATETIME.NOW,
		expires_at DATETIME NOT NULL,
		hits INTEGER DEFAULT 0
	);`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS bloom_filters(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bitset BLOB NOT NULL,
		size INTEGER NOT NULL,
		k INTEGER NOT NULL,
		cap INTEGER NOT NULL,
		count INTEGER NOT NULL
		);`); err != nil {
		return nil, err
	}
	return &Store{db}, nil
}

func (s *Store) InsertURL(original, short string, ttl time.Duration) error {
	now := time.Now()
	_, err := s.db.Exec(`
		INSERT INTO urls (original, short, created_at, expires_at)
		VALUES (?, ?, ?, ?)
		`, original, short, now, now.Add(ttl))
	return err
}

func (s *Store) GetByShort(short string) (*URL, error) {
	row := s.db.QueryRow(`
		SELECT id, original, short, created_at, expires_at FROM urls 
		WHERE 
			short = ? AND expires_at > ?
		`, short, time.Now())
	var url URL
	if err := row.Scan(&url.ID, &url.Original, &url.Short, &url.CreatedAt, &url.ExpiresAt); err != nil {
		return nil, err
	}
	return &url, nil
}

func (s *Store) IncrementHist(short string) error {
	_, err := s.db.Exec(`UPDATE urls SET hits = hits + 1 WHERE short = ?`, short)
	return err
}
