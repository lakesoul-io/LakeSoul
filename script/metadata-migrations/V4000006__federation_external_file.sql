-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

-- External file foreign tables backed by DataFusion ListingTable.
--
-- `backend` distinguishes the existing ADBC foreign servers from external file
-- servers. External-file scans carry their object-store configuration in the
-- distributed plan, so `driver` and `dialect` are placeholders for this
-- backend.

alter table if exists federation.servers
    add column if not exists backend text not null default 'adbc';
