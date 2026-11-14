"""A stand-in for an external payments API (think Stripe) with Idempotency-Key
support. It has its own database, so nothing it does can share a transaction
with jobq: from the queue's point of view it is truly external."""
