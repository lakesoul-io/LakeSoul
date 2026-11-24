-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
-- SPDX-License-Identifier: Apache-2.0

-- Run on the destination metadata PostgreSQL database.
INSERT INTO public.namespace (namespace, properties, comment)
VALUES ('lakesoul_meta', '{}', 'LakeSoul metadata mirror')
ON CONFLICT (namespace) DO NOTHING;
