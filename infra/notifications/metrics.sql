-- Aggregate diagnostics only: no contact fields, message bodies or recipient IDs.
SELECT count(*) AS pending_outbox,
       coalesce(extract(epoch FROM clock_timestamp() - min(created_at)), 0) AS oldest_outbox_seconds
FROM outbox WHERE published_at IS NULL;
SELECT topic, partition, next_offset FROM kafka_offsets ORDER BY topic, partition;
SELECT count(*) AS pending_without_profile FROM pending_reviews;
SELECT load_id, expected_count, completed_at,
       (SELECT count(*) FROM bootstrap_receipts r WHERE r.load_id = l.load_id) AS received,
       (SELECT count(*) FROM bootstrap_receipts r WHERE r.load_id = l.load_id AND applied_at IS NOT NULL) AS applied
FROM load_state l;
SELECT kind, count(*) AS unfinished,
       coalesce(extract(epoch FROM clock_timestamp() - min(period_start)), 0) AS oldest_schedule_seconds
FROM schedule_runs WHERE status <> 'completed' GROUP BY kind;
SELECT status, count(*) FROM deliveries GROUP BY status;
SELECT outcome, duplicate_risk, count(*) FROM delivery_attempts GROUP BY outcome, duplicate_risk;
SELECT terminal_reason, count(*) AS skipped_deliveries
FROM deliveries WHERE status IN ('skipped', 'cancelled') GROUP BY terminal_reason;

-- Compare classes independently: delayed work is not an overdue broker backlog.
SELECT d.send_class,
       count(*) FILTER (WHERE o.available_at <= clock_timestamp()) AS ready_outbox,
       count(*) FILTER (WHERE o.available_at > clock_timestamp()) AS future_outbox,
       coalesce(max(extract(epoch FROM clock_timestamp() - o.available_at))
                FILTER (WHERE o.available_at <= clock_timestamp()), 0) AS oldest_ready_seconds
FROM outbox o JOIN deliveries d USING (delivery_id)
WHERE o.published_at IS NULL GROUP BY d.send_class ORDER BY d.send_class;
SELECT send_class, status, count(*) FROM deliveries GROUP BY send_class, status;
SELECT d.send_class, a.outcome, a.duplicate_risk, count(*)
FROM delivery_attempts a JOIN deliveries d USING (delivery_id)
GROUP BY d.send_class, a.outcome, a.duplicate_risk;
