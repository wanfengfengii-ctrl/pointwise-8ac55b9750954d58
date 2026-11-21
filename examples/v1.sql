-- V1 source-site logical snapshot, loaded into a newly initialized source schema.
BEGIN IMMEDIATE;
INSERT INTO raw_batches (batch_id) VALUES ('LEGACY');
INSERT INTO raw_frames (batch_id, position, record_id, origin, frame)
VALUES ('LEGACY', 0, 'L01', 'R02 archived CH1', X'F8FF0400');
INSERT INTO sealed_batches (batch_id, digest)
VALUES ('LEGACY', '19b0c6e08e7d4e0d7fd984a63a7589f69a800537b42d0356e9d19e317a9ddc7c');
INSERT INTO sealed_entries (batch_id, position, record_id, origin, length, digest, peak)
VALUES ('LEGACY', 0, 'L01', 'R02 archived CH1', 4, 'd1e1a01edff7614e8e61e99722e117b591a7e18dafb5071d85364ec8fe52471a', 8);
INSERT INTO ordinary_requests (request_id, batch_id, record_id, status)
VALUES ('legacy-request', 'LEGACY', 'L01', 'published');
INSERT INTO consumed_records (batch_id, record_id, request_id)
VALUES ('LEGACY', 'L01', 'legacy-request');
INSERT INTO publications (sequence, publication_id, request_id, batch_id, record_id, origin, package)
VALUES (1, 'P00000001', 'legacy-request', 'LEGACY', 'L01', 'R02 archived CH1', CAST(
    '{"batch_digest":"19b0c6e08e7d4e0d7fd984a63a7589f69a800537b42d0356e9d19e317a9ddc7c","batch_id":"LEGACY","entry":{"batch_id":"LEGACY","digest":"d1e1a01edff7614e8e61e99722e117b591a7e18dafb5071d85364ec8fe52471a","length":4,"origin":"R02 archived CH1","peak":8,"position":0,"record_id":"L01"},"format":"waveform-v1","frame_b64":"+P8EAA==","origin":"R02 archived CH1","publication_id":"P00000001","record_id":"L01","request_id":"legacy-request"}'
    AS BLOB));
COMMIT;
