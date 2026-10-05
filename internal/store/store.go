// Package store is the Oracle persistence layer for templates, notifications,
// delivery attempts, the DLQ, rate-limit config and the transactional outbox.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	go_ora "github.com/sijms/go-ora/v2"

	"github.com/virend3rp/multi-channel-notification-service/internal/db"
	"github.com/virend3rp/multi-channel-notification-service/internal/domain"
	"github.com/virend3rp/multi-channel-notification-service/internal/messaging"
)

var (
	ErrNotFound  = errors.New("not found")
	ErrDuplicate = errors.New("duplicate")
	ErrConflict  = errors.New("conflict")
)

type Store struct {
	DB *sql.DB
}

func New(d *sql.DB) *Store { return &Store{DB: d} }

func clob(s string) go_ora.Clob { return go_ora.Clob{String: s, Valid: true} }

func nullStr(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

func now() time.Time { return time.Now().UTC() }

func boolToNum(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (s *Store) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// ---------------------------------------------------------------- templates

const templateCols = `id, code, channel, subject, body, version, active, created_at`

func scanTemplate(row interface{ Scan(...any) error }) (domain.Template, error) {
	var t domain.Template
	var subject sql.NullString
	var body go_ora.Clob
	var active int
	err := row.Scan(&t.ID, &t.Code, &t.Channel, &subject, &body, &t.Version, &active, &t.CreatedAt)
	t.Subject, t.Body, t.Active = subject.String, body.String, active == 1
	return t, err
}

func (s *Store) ActiveTemplate(ctx context.Context, code string) (domain.Template, error) {
	t, err := scanTemplate(s.DB.QueryRowContext(ctx,
		`SELECT `+templateCols+` FROM template WHERE code = :1 AND active = 1`, code))
	if errors.Is(err, sql.ErrNoRows) {
		return t, ErrNotFound
	}
	return t, err
}

// ListTemplates returns the active version of every template.
func (s *Store) ListTemplates(ctx context.Context) ([]domain.Template, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT `+templateCols+` FROM template WHERE active = 1 ORDER BY code`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Template
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) TemplateVersions(ctx context.Context, code string) ([]domain.Template, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT `+templateCols+` FROM template WHERE code = :1 ORDER BY version DESC`, code)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Template
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if len(out) == 0 && rows.Err() == nil {
		return nil, ErrNotFound
	}
	return out, rows.Err()
}

func (s *Store) CreateTemplate(ctx context.Context, t domain.Template) error {
	_, err := s.DB.ExecContext(ctx,
		`INSERT INTO template (id, code, channel, subject, body, version, active, created_at)
		 SELECT :1, :2, :3, :4, :5, 1, 1, :6 FROM dual
		 WHERE NOT EXISTS (SELECT 1 FROM template WHERE code = :7)`,
		t.ID, t.Code, string(t.Channel), nullStr(t.Subject), clob(t.Body), now(), t.Code)
	if err != nil {
		if db.IsUniqueViolation(err) {
			return ErrConflict
		}
		return err
	}
	return nil
}

// AddTemplateVersion stores a new version of an existing template and makes it the active one.
// The unique (code, version) constraint turns a concurrent edit into ErrConflict.
func (s *Store) AddTemplateVersion(ctx context.Context, id, code, subject, body string) (domain.Template, error) {
	var created domain.Template
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		cur, err := scanTemplate(tx.QueryRowContext(ctx,
			`SELECT `+templateCols+` FROM template WHERE code = :1 AND active = 1 FOR UPDATE`, code))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var maxVer int
		if err := tx.QueryRowContext(ctx, `SELECT MAX(version) FROM template WHERE code = :1`, code).Scan(&maxVer); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE template SET active = 0 WHERE code = :1 AND active = 1`, code); err != nil {
			return err
		}
		created = domain.Template{ID: id, Code: code, Channel: cur.Channel, Subject: subject, Body: body,
			Version: maxVer + 1, Active: true, CreatedAt: now()}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO template (id, code, channel, subject, body, version, active, created_at)
			 VALUES (:1, :2, :3, :4, :5, :6, 1, :7)`,
			created.ID, code, string(cur.Channel), nullStr(subject), clob(body), created.Version, created.CreatedAt)
		if db.IsUniqueViolation(err) {
			return ErrConflict
		}
		return err
	})
	return created, err
}

// ActivateTemplateVersion rolls a template back (or forward) to an existing version.
func (s *Store) ActivateTemplateVersion(ctx context.Context, code string, version int) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM template WHERE code = :1 AND version = :2`, code, version).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		if _, err := tx.ExecContext(ctx, `UPDATE template SET active = 0 WHERE code = :1 AND active = 1`, code); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE template SET active = 1 WHERE code = :1 AND version = :2`, code, version)
		return err
	})
}

// ------------------------------------------------------------ notifications

const notificationCols = `id, message_id, request_hash, channel, recipient, template_code, template_ver,
	payload_json, subject, body, status, priority, attempts, last_error, provider_ref, created_at, updated_at`

func scanNotification(row interface{ Scan(...any) error }) (domain.Notification, error) {
	var n domain.Notification
	var payload, subject, lastErr, providerRef sql.NullString
	var body go_ora.Clob
	err := row.Scan(&n.ID, &n.MessageID, &n.RequestHash, &n.Channel, &n.Recipient, &n.TemplateCode, &n.TemplateVer,
		&payload, &subject, &body, &n.Status, &n.Priority, &n.Attempts, &lastErr, &providerRef, &n.CreatedAt, &n.UpdatedAt)
	if err != nil {
		return n, err
	}
	n.PayloadJSON, n.Subject, n.Body = payload.String, subject.String, body.String
	n.LastError, n.ProviderRef = lastErr.String, providerRef.String
	if n.PayloadJSON != "" {
		_ = json.Unmarshal([]byte(n.PayloadJSON), &n.Payload)
	}
	return n, nil
}

// CreateNotification inserts the notification and its outbox message in one transaction.
// A repeated message_id (idempotency key) returns ErrDuplicate and writes nothing.
func (s *Store) CreateNotification(ctx context.Context, n domain.Notification, msg messaging.Message) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO notification (id, message_id, request_hash, channel, recipient, template_code, template_ver,
				payload_json, subject, body, status, priority, attempts, created_at, updated_at)
			 VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, 0, :13, :14)`,
			n.ID, n.MessageID, n.RequestHash, string(n.Channel), n.Recipient, n.TemplateCode, n.TemplateVer,
			nullStr(n.PayloadJSON), nullStr(n.Subject), clob(n.Body), string(n.Status), string(n.Priority),
			n.CreatedAt, n.UpdatedAt)
		if db.IsUniqueViolation(err) {
			return ErrDuplicate
		}
		if err != nil {
			return err
		}
		return insertOutbox(ctx, tx, msg)
	})
}

func insertOutbox(ctx context.Context, tx *sql.Tx, msg messaging.Message) error {
	headers, _ := json.Marshal(msg.Headers)
	_, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (topic, msg_key, payload, headers_json, created_at) VALUES (:1, :2, :3, :4, :5)`,
		msg.Topic, string(msg.Key), clob(string(msg.Value)), string(headers), now())
	return err
}

func (s *Store) NotificationByMessageID(ctx context.Context, messageID string) (domain.Notification, error) {
	n, err := scanNotification(s.DB.QueryRowContext(ctx,
		`SELECT `+notificationCols+` FROM notification WHERE message_id = :1`, messageID))
	if errors.Is(err, sql.ErrNoRows) {
		return n, ErrNotFound
	}
	return n, err
}

// Notification loads a notification together with its delivery attempts.
func (s *Store) Notification(ctx context.Context, id string) (domain.Notification, error) {
	n, err := scanNotification(s.DB.QueryRowContext(ctx,
		`SELECT `+notificationCols+` FROM notification WHERE id = :1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return n, ErrNotFound
	}
	if err != nil {
		return n, err
	}
	n.History, err = s.Attempts(ctx, id)
	return n, err
}

func (s *Store) Attempts(ctx context.Context, notificationID string) ([]domain.DeliveryAttempt, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT id, notification_id, attempt_no, provider, outcome, error_code, error_message, latency_ms, attempted_at
		 FROM delivery_attempt WHERE notification_id = :1 ORDER BY attempted_at`, notificationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.DeliveryAttempt{}
	for rows.Next() {
		var a domain.DeliveryAttempt
		var code, msg sql.NullString
		if err := rows.Scan(&a.ID, &a.NotificationID, &a.AttemptNo, &a.Provider, &a.Outcome, &code, &msg, &a.LatencyMs, &a.AttemptedAt); err != nil {
			return nil, err
		}
		a.ErrorCode, a.ErrorMessage = code.String, msg.String
		out = append(out, a)
	}
	return out, rows.Err()
}

type NotificationFilter struct {
	Channel   domain.Channel
	Status    domain.Status
	Recipient string
	From, To  time.Time
	Limit     int
	Offset    int
}

type Page[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
}

func (s *Store) ListNotifications(ctx context.Context, f NotificationFilter) (Page[domain.Notification], error) {
	var where []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Channel != "" {
		add("channel = :%d", string(f.Channel))
	}
	if f.Status != "" {
		add("status = :%d", string(f.Status))
	}
	if f.Recipient != "" {
		add("recipient = :%d", f.Recipient)
	}
	if !f.From.IsZero() {
		add("created_at >= :%d", f.From.UTC())
	}
	if !f.To.IsZero() {
		add("created_at < :%d", f.To.UTC())
	}
	cond := ""
	if len(where) > 0 {
		cond = " WHERE " + strings.Join(where, " AND ")
	}

	var page Page[domain.Notification]
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM notification`+cond, args...).Scan(&page.Total); err != nil {
		return page, err
	}
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 50
	}
	q := fmt.Sprintf(`SELECT %s FROM notification%s ORDER BY created_at DESC OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`,
		notificationCols, cond, f.Offset, f.Limit)
	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	page.Items = []domain.Notification{}
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return page, err
		}
		page.Items = append(page.Items, n)
	}
	return page, rows.Err()
}

// ApplyDeliveryReceipt records a provider callback. Only SENT notifications move,
// so late or duplicate webhooks cannot overwrite a newer state.
func (s *Store) ApplyDeliveryReceipt(ctx context.Context, messageID string, to domain.Status, errorCode string) (bool, error) {
	res, err := s.DB.ExecContext(ctx,
		`UPDATE notification SET status = :1, last_error = NVL(:2, last_error), updated_at = :3
		 WHERE message_id = :4 AND status = 'SENT'`,
		string(to), nullStr(errorCode), now(), messageID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// ------------------------------------------------------------- worker side

// Claim marks a notification SENDING for the given attempt. It succeeds when
//   - this attempt number is newer than the last one made (attempts < n) and the
//     notification is still in flight. SENDING is included because a worker publishes the
//     next retry message before it marks the row RETRYING; if it crashes in between, the
//     retry message must still be able to proceed; or
//   - the same attempt was claimed earlier and the worker crashed mid-send (re-entrant).
//
// It fails for anything already SENT, DELIVERED, FAILED or DEAD_LETTERED, and for stale
// duplicates of an older attempt. That is the consumer side of idempotency: a Kafka
// redelivery after a rebalance never re-sends a message the provider already accepted.
func (s *Store) Claim(ctx context.Context, id string, attempt int) (bool, domain.Status, error) {
	res, err := s.DB.ExecContext(ctx,
		`UPDATE notification SET status = 'SENDING', attempts = :1, updated_at = :2
		 WHERE id = :3 AND (
		   (attempts < :4 AND status IN ('QUEUED', 'RETRYING', 'SENDING'))
		   OR (attempts = :5 AND status = 'SENDING'))`,
		attempt, now(), id, attempt, attempt)
	if err != nil {
		return false, "", err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return true, domain.StatusSending, nil
	}
	var st domain.Status
	err = s.DB.QueryRowContext(ctx, `SELECT status FROM notification WHERE id = :1`, id).Scan(&st)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", ErrNotFound
	}
	return false, st, err
}

func (s *Store) RecordAttempt(ctx context.Context, a domain.DeliveryAttempt) error {
	_, err := s.DB.ExecContext(ctx,
		`INSERT INTO delivery_attempt (id, notification_id, attempt_no, provider, outcome, error_code, error_message, latency_ms, attempted_at)
		 VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)`,
		a.ID, a.NotificationID, a.AttemptNo, a.Provider, string(a.Outcome), nullStr(a.ErrorCode),
		nullStr(truncate(a.ErrorMessage, 500)), a.LatencyMs, a.AttemptedAt.UTC())
	return err
}

func (s *Store) MarkSent(ctx context.Context, id, providerRef string) error {
	return s.setStatus(ctx, id, domain.StatusSent, "", providerRef)
}

func (s *Store) MarkRetrying(ctx context.Context, id, lastError string) error {
	return s.setStatus(ctx, id, domain.StatusRetrying, lastError, "")
}

func (s *Store) MarkFailed(ctx context.Context, id, lastError string) error {
	return s.setStatus(ctx, id, domain.StatusFailed, lastError, "")
}

func (s *Store) setStatus(ctx context.Context, id string, st domain.Status, lastError, providerRef string) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE notification SET status = :1, last_error = NVL(:2, last_error), provider_ref = NVL(:3, provider_ref), updated_at = :4
		 WHERE id = :5 AND status = 'SENDING'`,
		string(st), nullStr(truncate(lastError, 500)), nullStr(providerRef), now(), id)
	return err
}

// DeadLetter marks the notification DEAD_LETTERED and stores the message for replay.
func (s *Store) DeadLetter(ctx context.Context, id string, channel domain.Channel, reason string, payload []byte) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE notification SET status = 'DEAD_LETTERED', last_error = :1, updated_at = :2
			 WHERE id = :3 AND status = 'SENDING'`, truncate(reason, 500), now(), id)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return nil // already dead-lettered by a duplicate delivery
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO dlq_message (id, notification_id, channel, reason, payload, created_at) VALUES (:1, :2, :3, :4, :5, :6)`,
			newID(), id, string(channel), truncate(reason, 500), clob(string(payload)), now())
		return err
	})
}

// ---------------------------------------------------------------------- DLQ

func (s *Store) ListDLQ(ctx context.Context, includeReplayed bool, limit, offset int) (Page[domain.DLQMessage], error) {
	cond := " WHERE replayed_at IS NULL"
	if includeReplayed {
		cond = ""
	}
	var page Page[domain.DLQMessage]
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM dlq_message`+cond).Scan(&page.Total); err != nil {
		return page, err
	}
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(
		`SELECT id, notification_id, channel, reason, payload, created_at, replayed_at FROM dlq_message%s
		 ORDER BY created_at DESC OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, cond, offset, limit))
	if err != nil {
		return page, err
	}
	defer rows.Close()
	page.Items = []domain.DLQMessage{}
	for rows.Next() {
		var m domain.DLQMessage
		var payload go_ora.Clob
		var replayed sql.NullTime
		if err := rows.Scan(&m.ID, &m.NotificationID, &m.Channel, &m.Reason, &payload, &m.CreatedAt, &replayed); err != nil {
			return page, err
		}
		m.Payload = payload.String
		if replayed.Valid {
			t := replayed.Time
			m.ReplayedAt = &t
		}
		page.Items = append(page.Items, m)
	}
	return page, rows.Err()
}

// ReplayDLQ re-queues a dead-lettered notification with a fresh attempt budget. The
// status reset, the new outbox message and the replayed_at stamp commit together.
func (s *Store) ReplayDLQ(ctx context.Context, dlqID string) (string, error) {
	var notificationID string
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var payload go_ora.Clob
		var replayed sql.NullTime
		err := tx.QueryRowContext(ctx,
			`SELECT notification_id, payload, replayed_at FROM dlq_message WHERE id = :1 FOR UPDATE`, dlqID).
			Scan(&notificationID, &payload, &replayed)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if replayed.Valid {
			return ErrConflict
		}
		env, err := messaging.UnmarshalEnvelope([]byte(payload.String))
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE notification SET status = 'QUEUED', attempts = 0, updated_at = :1
			 WHERE id = :2 AND status IN ('DEAD_LETTERED', 'FAILED')`, now(), notificationID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrConflict
		}
		if err := insertOutbox(ctx, tx, messaging.NewMainMessage(env)); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE dlq_message SET replayed_at = :1 WHERE id = :2`, now(), dlqID)
		return err
	})
	return notificationID, err
}

// ------------------------------------------------------------------- stats

func (s *Store) Stats(ctx context.Context, since time.Time) ([]domain.ChannelStats, error) {
	byChannel := map[domain.Channel]*domain.ChannelStats{}
	for _, c := range domain.AllChannels {
		byChannel[c] = &domain.ChannelStats{Channel: c, ByStatus: map[domain.Status]int64{}}
	}

	rows, err := s.DB.QueryContext(ctx,
		`SELECT channel, status, COUNT(*) FROM notification WHERE created_at >= :1 GROUP BY channel, status`, since.UTC())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c domain.Channel
		var st domain.Status
		var n int64
		if err := rows.Scan(&c, &st, &n); err != nil {
			rows.Close()
			return nil, err
		}
		if cs := byChannel[c]; cs != nil {
			cs.ByStatus[st] = n
		}
	}
	rows.Close()

	rows, err = s.DB.QueryContext(ctx,
		`SELECT n.channel, COUNT(*),
		        SUM(CASE WHEN a.outcome <> 'SUCCESS' THEN 1 ELSE 0 END),
		        NVL(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY a.latency_ms), 0),
		        NVL(AVG(a.latency_ms), 0)
		 FROM delivery_attempt a JOIN notification n ON n.id = a.notification_id
		 WHERE a.attempted_at >= :1 GROUP BY n.channel`, since.UTC())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c domain.Channel
		var attempts, failed int64
		var p95, avg float64
		if err := rows.Scan(&c, &attempts, &failed, &p95, &avg); err != nil {
			rows.Close()
			return nil, err
		}
		if cs := byChannel[c]; cs != nil {
			cs.Attempts, cs.FailedTries, cs.P95LatencyMs, cs.AvgLatencyMs = attempts, failed, p95, avg
		}
	}
	rows.Close()

	rows, err = s.DB.QueryContext(ctx, `SELECT channel, COUNT(*) FROM dlq_message WHERE replayed_at IS NULL GROUP BY channel`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var c domain.Channel
		var n int64
		if err := rows.Scan(&c, &n); err != nil {
			return nil, err
		}
		if cs := byChannel[c]; cs != nil {
			cs.DLQPending = n
		}
	}

	out := make([]domain.ChannelStats, 0, len(domain.AllChannels))
	for _, c := range domain.AllChannels {
		out = append(out, *byChannel[c])
	}
	return out, rows.Err()
}

// ------------------------------------------------------------- rate limits

func (s *Store) RateLimits(ctx context.Context) ([]domain.RateLimit, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT channel, permits_per_sec, burst FROM rate_limit_config ORDER BY channel`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.RateLimit
	for rows.Next() {
		var r domain.RateLimit
		if err := rows.Scan(&r.Channel, &r.PermitsPerSec, &r.Burst); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) SetRateLimit(ctx context.Context, r domain.RateLimit) error {
	_, err := s.DB.ExecContext(ctx,
		`MERGE INTO rate_limit_config t USING (SELECT :1 AS channel, :2 AS pps, :3 AS burst FROM dual) s
		 ON (t.channel = s.channel)
		 WHEN MATCHED THEN UPDATE SET t.permits_per_sec = s.pps, t.burst = s.burst
		 WHEN NOT MATCHED THEN INSERT (channel, permits_per_sec, burst) VALUES (s.channel, s.pps, s.burst)`,
		string(r.Channel), r.PermitsPerSec, r.Burst)
	return err
}

// ------------------------------------------------------------------ outbox

type OutboxRow struct {
	Seq     int64
	Message messaging.Message
}

// UnpublishedOutbox returns the oldest unpublished rows in insertion order. The relay
// runs as a single instance, so ordering by seq preserves per-recipient order.
func (s *Store) UnpublishedOutbox(ctx context.Context, limit int) ([]OutboxRow, error) {
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(
		`SELECT seq, topic, msg_key, payload, headers_json FROM outbox
		 WHERE published_at IS NULL ORDER BY seq FETCH FIRST %d ROWS ONLY`, limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxRow
	for rows.Next() {
		var r OutboxRow
		var key string
		var payload go_ora.Clob
		var headers sql.NullString
		if err := rows.Scan(&r.Seq, &r.Message.Topic, &key, &payload, &headers); err != nil {
			return nil, err
		}
		r.Message.Key, r.Message.Value = []byte(key), []byte(payload.String)
		if headers.Valid {
			_ = json.Unmarshal([]byte(headers.String), &r.Message.Headers)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkOutboxPublished stamps exactly the rows that were sent. Marking a seq range instead
// could cover a row whose transaction committed after the read and was never published.
func (s *Store) MarkOutboxPublished(ctx context.Context, seqs []int64) error {
	if len(seqs) == 0 {
		return nil
	}
	args := []any{now()}
	ph := make([]string, len(seqs))
	for i, seq := range seqs {
		args = append(args, seq)
		ph[i] = fmt.Sprintf(":%d", i+2)
	}
	_, err := s.DB.ExecContext(ctx,
		`UPDATE outbox SET published_at = :1 WHERE seq IN (`+strings.Join(ph, ", ")+`)`, args...)
	return err
}

// PurgeOutbox deletes published rows older than the retention window.
func (s *Store) PurgeOutbox(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.DB.ExecContext(ctx, `DELETE FROM outbox WHERE published_at < :1`, olderThan.UTC())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[:n]
	for !utf8.ValidString(s) { // never cut a multi-byte character in half
		s = s[:len(s)-1]
	}
	return s
}

func newID() string { return uuid.NewString() }
