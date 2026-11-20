-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

-- Deferred secondary-index maintenance.
--
-- A single row serializes the background builder across coordinators and an
-- external Kubernetes runner. The lease expires, so a crashed runner cannot
-- block maintenance forever.
create schema if not exists maintenance;

create table if not exists maintenance.lease
(
    name             text primary key,
    owner            text,
    lease_expires_at bigint,
    updated_at       bigint not null
);
