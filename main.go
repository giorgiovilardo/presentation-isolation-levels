package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/chzyer/readline"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/lmittmann/tint"
)

func main() {
	ctx := context.Background()

	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{TimeFormat: time.TimeOnly}),
	))

	pool := connectToDatabase(ctx)
	defer pool.Close()

	if err := initDatabase(ctx, pool); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to initialize database: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Connected to database with max pool size = %d. Type 'exit' to quit.\n", pool.Config().MaxConns)
	fmt.Printf("Available commands: nrr, serializable, sfu, queue\n")
	runREPL(ctx, pool)
}

func connectToDatabase(ctx context.Context) *pgxpool.Pool {
	config, err := pgxpool.ParseConfig("postgres://demo:demo@localhost:7777/isolation_demo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to parse config: %v\n", err)
		os.Exit(1)
	}

	config.ConnConfig.Tracer = &tracelog.TraceLog{
		Logger: tracelog.LoggerFunc(func(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
			if msg == "Query" {
				sql := data["sql"].(string)
				sql = strings.ReplaceAll(sql, "\n", " ")
				sql = strings.ReplaceAll(sql, "\t", " ")
				for strings.Contains(sql, "  ") {
					sql = strings.ReplaceAll(sql, "  ", " ")
				}
				sql = strings.TrimSpace(sql)
				slog.Info("DB Query:", "PID", data["pid"], "SQL", sql)
			}
		}),
		LogLevel: tracelog.LogLevelInfo,
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}

	if err := pool.Ping(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to ping database: %v\n", err)
		os.Exit(1)
	}

	return pool
}

func runREPL(ctx context.Context, pool *pgxpool.Pool) {
	rl, err := readline.New(" >> ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize readline: %v\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			return
		}

		command := strings.TrimSpace(line)

		switch command {
		case "":
			continue
		case "exit":
			return
		case "nrr":
			demonstrateNonRepeatableRead(ctx, pool)
		case "serializable":
			demonstrateSerializable(ctx, pool)
		case "sfu":
			demonstrateSelectForUpdate(ctx, pool)
		case "queue":
			demonstrateQueueProcessing(ctx, pool)
		default:
			fmt.Printf("Unknown command: %s\n", command)
		}
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// isSerializationError checks if an error is a serialization failure
func isSerializationError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
		return true
	}
	return strings.Contains(err.Error(), "commit unexpectedly resulted in rollback")
}

// txFunc is a function that operates within a transaction
type txFunc func(context.Context, pgx.Tx) error

// withTransaction executes a function within a transaction with proper cleanup
func withTransaction(ctx context.Context, pool *pgxpool.Pool, isoLevel pgx.TxIsoLevel, fn txFunc) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: isoLevel})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := fn(ctx, tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// productInfo holds product data
type productInfo struct {
	name  string
	price float64
	stock int
}

// getProductInfo retrieves product information
func getProductInfo(ctx context.Context, queryable interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, productID int) (productInfo, error) {
	var p productInfo
	err := queryable.QueryRow(ctx, "SELECT name, price, stock_quantity FROM products WHERE id = $1", productID).Scan(&p.name, &p.price, &p.stock)
	return p, err
}

// eventProcessResult represents the outcome of event processing
type eventProcessResult int

const (
	eventProcessed eventProcessResult = iota
	eventSkipped
	eventNoMore
	eventError
)

// getNextPendingEvent retrieves the next pending event using SKIP LOCKED
func getNextPendingEvent(ctx context.Context, tx pgx.Tx) (int, string, error) {
	var eventID int
	var eventName string
	query := `SELECT id, event_name FROM events WHERE status = 'pending' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`
	err := tx.QueryRow(ctx, query).Scan(&eventID, &eventName)
	return eventID, eventName, err
}

// tryAcquireAdvisoryLock attempts to acquire an advisory lock
func tryAcquireAdvisoryLock(ctx context.Context, tx pgx.Tx, lockID int) (bool, error) {
	var gotLock bool
	err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", lockID).Scan(&gotLock)
	return gotLock, err
}

// markEventCompleted marks an event as completed
func markEventCompleted(ctx context.Context, tx pgx.Tx, eventID, workerID int) error {
	_, err := tx.Exec(ctx, `
		UPDATE events
		SET status = 'completed',
		    processed_at = NOW(),
		    processed_by = $1
		WHERE id = $2
	`, workerID, eventID)
	return err
}

// processNextEvent processes the next event in the queue using SKIP LOCKED
func processNextEvent(ctx context.Context, pool *pgxpool.Pool, workerID int, startTime time.Time) (eventProcessResult, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return eventError, err
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return eventError, err
	}
	defer tx.Rollback(ctx)

	// Try to get next event
	eventID, eventName, err := getNextPendingEvent(ctx, tx)
	if err == pgx.ErrNoRows {
		elapsed := time.Since(startTime).Round(time.Millisecond)
		slog.Info("No more events", "worker", workerID, "time", elapsed)
		return eventNoMore, nil
	}
	if err != nil {
		return eventError, err
	}

	// Try to acquire advisory lock
	gotLock, err := tryAcquireAdvisoryLock(ctx, tx, eventID)
	if err != nil || !gotLock {
		return eventSkipped, nil
	}

	elapsed := time.Since(startTime).Round(time.Millisecond)
	slog.Info("✓ Claimed event", "worker", workerID, "event", eventName, "id", eventID, "time", elapsed)

	// Process event
	time.Sleep(2 * time.Second)

	// Mark as completed
	if err := markEventCompleted(ctx, tx, eventID, workerID); err != nil {
		return eventError, err
	}

	if err := tx.Commit(ctx); err != nil {
		return eventError, err
	}

	elapsed = time.Since(startTime).Round(time.Millisecond)
	slog.Info("✓ Completed event", "worker", workerID, "event", eventName, "id", eventID, "time", elapsed)
	return eventProcessed, nil
}

// lockStrategy defines SQL query and log message for different lock modes
type lockStrategy struct {
	query      string
	logMessage string
}

var lockStrategies = map[string]lockStrategy{
	"none": {
		query:      "SELECT name, price, stock_quantity FROM products WHERE id = $1",
		logMessage: "Read (regular)",
	},
	"forupdate": {
		query:      "SELECT name, price, stock_quantity FROM products WHERE id = $1 FOR UPDATE",
		logMessage: "Read with FOR UPDATE",
	},
	"nowait": {
		query:      "SELECT name, price, stock_quantity FROM products WHERE id = $1 FOR UPDATE NOWAIT",
		logMessage: "Read with FOR UPDATE NOWAIT",
	},
}

// =============================================================================
// DATABASE INITIALIZATION
// =============================================================================

func initDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	pool.Exec(ctx, "DROP TABLE IF EXISTS products")

	createTableSQL := `
		CREATE TABLE products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			price DECIMAL(10, 2) NOT NULL,
			stock_quantity INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`
	pool.Exec(ctx, createTableSQL)

	insertSQL := `
		INSERT INTO products (name, price, stock_quantity) VALUES
			('Laptop', 999.99, 10),
			('Concert ticket', 19.99, 1),
			('Wireless Mouse', 29.99, 50),
			('Mechanical Keyboard', 149.99, 25),
			('USB-C Hub', 49.99, 30),
			('Monitor 27"', 399.99, 15)
	`
	pool.Exec(ctx, insertSQL)

	// Create events table for queue processing demo
	pool.Exec(ctx, "DROP TABLE IF EXISTS events")

	createEventsSQL := `
		CREATE TABLE events (
			id SERIAL PRIMARY KEY,
			event_name VARCHAR(100) NOT NULL,
			status VARCHAR(20) DEFAULT 'pending',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			processed_at TIMESTAMP,
			processed_by INTEGER
		)
	`
	pool.Exec(ctx, createEventsSQL)

	pool.Exec(ctx, "CREATE INDEX idx_events_status_id ON events(status, id)")

	insertEventsSQL := `
		INSERT INTO events (event_name) VALUES
			('UserRegistered'),
			('OrderPlaced'),
			('EmailSent'),
			('PaymentProcessed'),
			('InventoryAdjusted'),
			('OrderShipped'),
			('UserEmailVerified'),
			('NotificationQueued'),
			('ReportGenerated')
	`
	pool.Exec(ctx, insertEventsSQL)

	slog.Info("Database initialized successfully")
	return nil
}

// =============================================================================
// DEMO 1: NON-REPEATABLE READ
// =============================================================================

func demonstrateNonRepeatableRead(ctx context.Context, pool *pgxpool.Pool) {
	logDemoHeader("Non-Repeatable Read Demonstration")

	logScenarioHeader("Scenario 1: READ COMMITTED (allows non-repeatable read)")
	runNonRepeatableReadScenario(ctx, pool, "read committed")

	time.Sleep(2 * time.Second)
	slog.Info("")

	logScenarioHeader("Scenario 2: REPEATABLE READ (prevents non-repeatable read)")
	runNonRepeatableReadScenario(ctx, pool, "repeatable read")
}

func runNonRepeatableReadScenario(ctx context.Context, pool *pgxpool.Pool, isolationLevel string) {
	const productID = 2

	tx1ReadFirst := make(chan struct{})
	tx2Updated := make(chan struct{})
	done := make(chan struct{}, 2)

	isoLevel := pgx.ReadCommitted
	if isolationLevel == "repeatable read" {
		isoLevel = pgx.RepeatableRead
	}

	// Transaction 1: Reads twice
	go func() {
		defer func() { done <- struct{}{} }()

		err := withTransaction(ctx, pool, isoLevel, func(ctx context.Context, tx pgx.Tx) error {
			product, err := getProductInfo(ctx, tx, productID)
			if err != nil {
				return err
			}

			slog.Info("TX1: First read", "product", product.name, "price", product.price, "stock", product.stock)
			close(tx1ReadFirst)

			<-tx2Updated
			time.Sleep(500 * time.Millisecond)

			product, err = getProductInfo(ctx, tx, productID)
			if err != nil {
				return err
			}

			slog.Info("TX1: Second read", "product", product.name, "price", product.price, "stock", product.stock)
			return nil
		})

		if err != nil {
			slog.Error("TX1 failed", "error", err)
		}
	}()

	// Transaction 2: Updates between TX1's reads
	go func() {
		defer func() { done <- struct{}{} }()

		<-tx1ReadFirst
		time.Sleep(100 * time.Millisecond)

		err := withTransaction(ctx, pool, isoLevel, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx,
				"UPDATE products SET price = price + 10.00, stock_quantity = stock_quantity - 1 WHERE id = $1",
				productID,
			)
			return err
		})

		if err != nil {
			slog.Error("TX2 failed", "error", err)
		}
		close(tx2Updated)
	}()

	<-done
	<-done

	resetData(ctx, pool, productID, 19.99, 1)
}

// =============================================================================
// DEMO 2: SERIALIZABLE ISOLATION
// =============================================================================

func demonstrateSerializable(ctx context.Context, pool *pgxpool.Pool) {
	logDemoHeader("Serializable Isolation Demonstration")

	const productID = 2 // Concert ticket (only 1 in stock)
	const numBuyers = 5

	logScenarioHeader("Scenario 1: READ COMMITTED (default)")
	slog.Info("5 concurrent customers trying to buy the last concert ticket...")
	slog.Info("")
	runBuyingScenario(ctx, pool, productID, numBuyers, pgx.ReadCommitted, false)
	resetStock(ctx, pool, productID, 1)

	time.Sleep(2 * time.Second)
	slog.Info("")

	logScenarioHeader("Scenario 2: SERIALIZABLE (no retry)")
	slog.Info("5 concurrent customers trying to buy the last concert ticket...")
	slog.Info("")
	runBuyingScenario(ctx, pool, productID, numBuyers, pgx.Serializable, false)
	resetStock(ctx, pool, productID, 1)
}

func runBuyingScenario(ctx context.Context, pool *pgxpool.Pool, productID, numBuyers int, isoLevel pgx.TxIsoLevel, withRetry bool) {
	var successCount atomic.Int32
	done := make(chan struct{}, numBuyers)
	startTime := time.Now()

	for i := range numBuyers {
		buyerID := i + 1
		go func(id int) {
			maxRetries := 1
			if withRetry {
				maxRetries = 5
			}

			for attempt := 0; attempt < maxRetries; attempt++ {
				if attempt > 0 {
					backoff := time.Duration(attempt*attempt*10) * time.Millisecond
					time.Sleep(backoff)
				}

				conn, err := pool.Acquire(ctx)
				if err != nil {
					slog.Error("Failed to acquire connection", "buyer", id, "error", err)
					done <- struct{}{}
					return
				}

				tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: isoLevel})
				if err != nil {
					conn.Release()
					slog.Error("Failed to begin transaction", "buyer", id, "error", err)
					done <- struct{}{}
					return
				}

				var stock int
				err = tx.QueryRow(ctx,
					"SELECT stock_quantity FROM products WHERE id = $1",
					productID,
				).Scan(&stock)

				if err != nil {
					tx.Rollback(ctx)
					conn.Release()
					slog.Error("Failed to query stock", "buyer", id, "error", err)
					done <- struct{}{}
					return
				}

				if stock > 0 {
					time.Sleep(10 * time.Millisecond)

					_, err = tx.Exec(ctx,
						"UPDATE products SET stock_quantity = stock_quantity - 1 WHERE id = $1",
						productID,
					)

					if err != nil {
						tx.Rollback(ctx)
						conn.Release()
						slog.Error("Failed to update stock", "buyer", id, "error", err)
						done <- struct{}{}
						return
					}

					err := tx.Commit(ctx)
					conn.Release()

					if err != nil {
						if isSerializationError(err) {
							elapsed := time.Since(startTime).Round(time.Millisecond)
							if attempt < maxRetries-1 && withRetry {
								continue
							} else {
								slog.Info("Serialization failure!",
									"buyer", id,
									"time", elapsed,
									"error", "transaction aborted (40001)")
								break
							}
						} else {
							slog.Info("Commit error", "error", err.Error())
							break
						}
					} else {
						elapsed := time.Since(startTime).Round(time.Millisecond)
						slog.Info("✓ Successfully purchased", "buyer", id, "time", elapsed)
						successCount.Add(1)
						break
					}
				} else {
					elapsed := time.Since(startTime).Round(time.Millisecond)
					slog.Info("Out of stock", "buyer", id, "time", elapsed)
					tx.Rollback(ctx)
					conn.Release()
					break
				}
			}

			done <- struct{}{}
		}(buyerID)
	}

	for range numBuyers {
		<-done
	}

	logResults(ctx, pool, productID, successCount.Load(), isoLevel)
}

func logResults(ctx context.Context, pool *pgxpool.Pool, productID int, successCount int32, isoLevel pgx.TxIsoLevel) {
	slog.Info("")
	slog.Info("All transactions completed")
	slog.Info("")

	var finalStock int
	var name string
	pool.QueryRow(ctx,
		"SELECT name, stock_quantity FROM products WHERE id = $1",
		productID,
	).Scan(&name, &finalStock)

	slog.Info("Results", "product", name, "successful_purchases", successCount, "final_stock", finalStock)
	slog.Info("")

	if isoLevel == pgx.ReadCommitted {
		slog.Info("Expected: 1 successful purchase, final stock = 0")
		slog.Info("Problem: Race conditions allow multiple 'successful' purchases of last ticket!")
	} else {
		slog.Info("Expected: 1 successful purchase, final stock = 0")
		slog.Info("SERIALIZABLE prevents race conditions through transaction conflicts!")
	}
}

// =============================================================================
// DEMO 3: SELECT FOR UPDATE
// =============================================================================

func demonstrateSelectForUpdate(ctx context.Context, pool *pgxpool.Pool) {
	logDemoHeader("SELECT FOR UPDATE Demonstration")

	const productID = 2 // Concert ticket

	logScenarioHeader("Scenario 1: Regular SELECT (no locking)")
	runSelectForUpdateScenario(ctx, pool, productID, "none")
	resetData(ctx, pool, productID, 19.99, 1)

	time.Sleep(2 * time.Second)
	slog.Info("")

	logScenarioHeader("Scenario 2: SELECT FOR UPDATE (blocking)")
	runSelectForUpdateScenario(ctx, pool, productID, "forupdate")
	resetData(ctx, pool, productID, 19.99, 1)

	time.Sleep(2 * time.Second)
	slog.Info("")

	logScenarioHeader("Scenario 3: SELECT FOR UPDATE NOWAIT (fail fast)")
	runSelectForUpdateScenario(ctx, pool, productID, "nowait")
	resetData(ctx, pool, productID, 19.99, 1)
}

func runSelectForUpdateScenario(ctx context.Context, pool *pgxpool.Pool, productID int, lockMode string) {
	tx1Read := make(chan struct{})
	done := make(chan struct{}, 2)

	strategy := lockStrategies[lockMode]

	// Transaction 1: Read-modify-write pattern
	go func() {
		defer func() { done <- struct{}{} }()

		conn, err := pool.Acquire(ctx)
		if err != nil {
			slog.Error("TX1: Failed to acquire connection", "error", err)
			return
		}
		defer conn.Release()

		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			slog.Error("TX1: Failed to begin transaction", "error", err)
			return
		}
		defer tx.Rollback(ctx)

		var name string
		var price float64
		var stock int

		if err := tx.QueryRow(ctx, strategy.query, productID).Scan(&name, &price, &stock); err != nil {
			slog.Error("TX1: Read failed", "error", err)
			return
		}
		slog.Info(fmt.Sprintf("TX1: %s", strategy.logMessage), "product", name, "price", price, "stock", stock)

		close(tx1Read)

		time.Sleep(500 * time.Millisecond)
		slog.Info("TX1: Processing business logic...")

		newPrice := price + 10.00
		if _, err := tx.Exec(ctx, "UPDATE products SET price = $1 WHERE id = $2", newPrice, productID); err != nil {
			slog.Error("TX1: Update failed", "error", err)
			return
		}
		slog.Info("TX1: Updated price", "old_price", price, "new_price", newPrice)

		time.Sleep(1 * time.Second)

		if err := tx.Commit(ctx); err != nil {
			slog.Error("TX1: Commit failed", "error", err)
		}
	}()

	// Transaction 2: Same read-modify-write pattern
	go func() {
		defer func() { done <- struct{}{} }()

		<-tx1Read
		time.Sleep(100 * time.Millisecond)

		conn, err := pool.Acquire(ctx)
		if err != nil {
			slog.Error("TX2: Failed to acquire connection", "error", err)
			return
		}
		defer conn.Release()

		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			slog.Error("TX2: Failed to begin transaction", "error", err)
			return
		}
		defer tx.Rollback(ctx)

		var name string
		var price float64
		var stock int

		startTime := time.Now()
		if lockMode == "forupdate" {
			slog.Info("TX2: Attempting SELECT FOR UPDATE (will block)...")
		}

		readErr := tx.QueryRow(ctx, strategy.query, productID).Scan(&name, &price, &stock)
		elapsed := time.Since(startTime)

		if readErr != nil {
			if lockMode == "nowait" {
				slog.Info("TX2: FOR UPDATE NOWAIT failed immediately!", "error", readErr.Error(), "elapsed", elapsed.Round(time.Millisecond))
			} else {
				slog.Error("TX2: Read failed", "error", readErr)
			}
			return
		}

		if lockMode == "forupdate" {
			slog.Info("TX2: Read with FOR UPDATE (was blocked)", "product", name, "price", price, "stock", stock, "waited", elapsed.Round(time.Millisecond))
		} else {
			slog.Info(fmt.Sprintf("TX2: %s", strategy.logMessage), "product", name, "price", price, "stock", stock)
		}

		time.Sleep(300 * time.Millisecond)
		slog.Info("TX2: Processing business logic...")

		newPrice := price + 5.00
		if _, err := tx.Exec(ctx, "UPDATE products SET price = $1 WHERE id = $2", newPrice, productID); err != nil {
			slog.Error("TX2: Update failed", "error", err)
			return
		}
		slog.Info("TX2: Updated price", "old_price", price, "new_price", newPrice)

		if err := tx.Commit(ctx); err != nil {
			slog.Error("TX2: Commit failed", "error", err)
		}
	}()

	<-done
	<-done

	var finalPrice float64
	pool.QueryRow(ctx,
		"SELECT price FROM products WHERE id = $1",
		productID,
	).Scan(&finalPrice)

	slog.Info("")
	slog.Info("Final price in database", "price", finalPrice)
	slog.Info("")

	if lockMode == "forupdate" {
		slog.Info("Result: Both transactions saw correct data!")
		slog.Info("TX2 was blocked until TX1 committed, preventing lost update")
		slog.Info("Expected: 19.99 + 10 + 5 = 34.99")
	} else if lockMode == "nowait" {
		slog.Info("Result: TX2 failed immediately with lock error!")
		slog.Info("NOWAIT allows fast failure instead of blocking")
		slog.Info("Application can retry or handle error gracefully")
		slog.Info("Expected: 19.99 + 10 = 29.99 (only TX1 updated)")
	} else {
		slog.Info("Result: Lost update! TX2 overwrote TX1's change")
		slog.Info("Both read 19.99, TX1 set to 29.99, TX2 set to 24.99")
		slog.Info("Expected with FOR UPDATE: 34.99, Actual: 24.99")
	}
}

// =============================================================================
// DEMO 4: QUEUE PROCESSING WITH ADVISORY LOCKS
// =============================================================================

func demonstrateQueueProcessing(ctx context.Context, pool *pgxpool.Pool) {
	logDemoHeader("Queue Processing Demonstration")

	const numWorkers = 3

	logScenarioHeader("SELECT FOR UPDATE SKIP LOCKED + Advisory Locks")
	slog.Info("3 workers using SKIP LOCKED (no lock contention)...")
	slog.Info("")
	runQueueWorkers(ctx, pool, numWorkers)
	resetEvents(ctx, pool)
}

func runQueueWorkers(ctx context.Context, pool *pgxpool.Pool, numWorkers int) {
	done := make(chan struct{}, numWorkers)
	startTime := time.Now()
	var processedCount [4]atomic.Int32 // index 0 unused, workers are 1-3

	for i := 1; i <= numWorkers; i++ {
		workerID := i
		go func(id int) {
			defer func() { done <- struct{}{} }()

			for {
				result, err := processNextEvent(ctx, pool, id, startTime)

				switch result {
				case eventProcessed:
					processedCount[id].Add(1)
					continue // Process next event
				case eventSkipped:
					continue // Try next event
				case eventNoMore:
					return // All done
				case eventError:
					if err != nil {
						slog.Error("Error processing event", "worker", id, "error", err)
					}
					return
				}
			}
		}(workerID)
	}

	// Wait for all workers to finish
	for range numWorkers {
		<-done
	}

	slog.Info("")
	slog.Info("All workers finished")
	slog.Info("")

	// Show results
	var totalCompleted int
	pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM events WHERE status = 'completed'
	`).Scan(&totalCompleted)

	slog.Info("Results",
		"worker_1_processed", processedCount[1].Load(),
		"worker_2_processed", processedCount[2].Load(),
		"worker_3_processed", processedCount[3].Load(),
		"total_completed", totalCompleted,
	)
	slog.Info("")
	slog.Info("SKIP LOCKED prevented workers from seeing locked rows.")
	slog.Info("Advisory locks provided additional coordination layer.")
	slog.Info("This pattern efficiently distributes work without contention.")
}

func resetEvents(ctx context.Context, pool *pgxpool.Pool) {
	pool.Exec(ctx, `
		UPDATE events
		SET status = 'pending',
		    processed_at = NULL,
		    processed_by = NULL
	`)
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

func logDemoHeader(title string) {
	slog.Info(fmt.Sprintf("=== %s ===", title))
	slog.Info("")
}

func logScenarioHeader(title string) {
	slog.Info(fmt.Sprintf("--- %s ---", title))
}

func resetData(ctx context.Context, pool *pgxpool.Pool, productID int, price float64, stock int) {
	slog.Info("")
	slog.Info("Resetting data...")
	pool.Exec(ctx, "UPDATE products SET price = $1, stock_quantity = $2 WHERE id = $3", price, stock, productID)
}

func resetStock(ctx context.Context, pool *pgxpool.Pool, productID int, stock int) {
	slog.Info("")
	slog.Info("Resetting data...")
	pool.Exec(ctx, "UPDATE products SET stock_quantity = $1 WHERE id = $2", stock, productID)
}
