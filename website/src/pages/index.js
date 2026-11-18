// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

import React from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import CodeBlock from '@theme/CodeBlock';
import {translate} from '@docusaurus/Translate';
import GitHubButton from 'react-github-btn';
import LakeSoulLogo from '@site/static/img/LakeSoul_Horizontal_White.svg';
import styles from './index.module.css';

const GITHUB_URL = 'https://github.com/lakesoul-io/LakeSoul';
const QUICKSTART_URL = '/docs/get-started/quickstart-postgres';

const t = (id, message) => translate({id, message});

function Hero() {
  return (
    <header className={styles.hero}>
      <div className={styles.heroGlow} aria-hidden="true" />
      <div className={styles.heroInner}>
        <div className={styles.heroCopy}>
          <LakeSoulLogo className={styles.heroLogo} />
          <p className={styles.eyebrow}>
            {t('homepage.hero.eyebrow', 'LF AI & Data Sandbox Project')}
          </p>
          <h1 className={styles.heroTitle}>
            <span>{t('homepage.hero.line1', 'Postgres-native.')}</span>
            <span>{t('homepage.hero.line2', 'Vector-native.')}</span>
            <span>{t('homepage.hero.line3', 'Python-native.')}</span>
          </h1>
          <p className={styles.heroSubtitle}>
            {t(
              'homepage.hero.subtitle',
              'An open lakehouse engine — a shared catalog, ACID upserts, SQL, incremental compute, vector search and a Python SDK, in Rust, over your object storage.'
            )}
          </p>
          <div className={styles.actions}>
            <Link className={styles.primaryButton} to="/docs/get-started/what-is-lakesoul">
              {t('homepage.hero.ctaPrimary', 'Get started')}
            </Link>
            <Link className={styles.secondaryButton} href={GITHUB_URL}>
              GitHub
            </Link>
          </div>
          <div className={styles.star}>
            <GitHubButton
              href={GITHUB_URL}
              data-size="large"
              data-show-count="true"
              aria-label="Star LakeSoul on GitHub">
              Star
            </GitHubButton>
          </div>
        </div>

        <div className={styles.terminal}>
          <div className={styles.terminalBar}>
            <span className={styles.dotRed} />
            <span className={styles.dotYellow} />
            <span className={styles.dotGreen} />
            <em className={styles.terminalTitle}>psql</em>
          </div>
          <pre className={styles.terminalBody}>{`$ psql "postgresql://lakesoul@localhost:5432/default"

default=> CREATE TABLE orders (
default(>   id bigint primary key,
default(>   region text,
default(>   amount numeric(20,4)
default(> );
CREATE TABLE

default=> INSERT INTO orders VALUES (1, 'APAC', 42.00)
default->   ON CONFLICT (id) DO UPDATE
default->   SET region = EXCLUDED.region, amount = EXCLUDED.amount;
INSERT 0 1

default=> SELECT region, sum(amount) FROM orders GROUP BY 1;
 region |  sum
--------+-------
 APAC   | 42.00
(1 row)`}</pre>
        </div>
      </div>
    </header>
  );
}

function Section({title, subtitle, tinted, children}) {
  return (
    <section className={tinted ? `${styles.section} ${styles.tinted}` : styles.section}>
      <div className={styles.sectionInner}>
        <h2 className={styles.sectionTitle}>{title}</h2>
        {subtitle && <p className={styles.sectionSubtitle}>{subtitle}</p>}
        {children}
      </div>
    </section>
  );
}

export default function Home() {
  const pillars = [
    {
      tag: 'SQL',
      title: t('homepage.pillar1.title', 'Postgres-native'),
      body: t(
        'homepage.pillar1.body',
        'CREATE TABLE, INSERT ... ON CONFLICT and queries run in the engine. psql, JDBC and pg_catalog work, including psql \\d; no sidecar, no CDC pipeline.'
      ),
    },
    {
      tag: t('homepage.pillar2.tag', 'Incremental'),
      title: t('homepage.pillar2.title', 'Efficient incremental SQL'),
      body: t(
        'homepage.pillar2.body',
        'With incremental SQL enabled, INSERT ... SELECT updates its target from the commit log: only changed rows are written, and the result matches a full recompute.'
      ),
    },
    {
      tag: 'ANN',
      title: t('homepage.pillar3.title', 'Vector-native'),
      body: t(
        'homepage.pillar3.body',
        'pgvector-compatible SQL over IVF + RaBitQ indexes on object storage. Structured filters and ANN in one plan, with BM25 full-text search beside it.'
      ),
    },
    {
      tag: 'Python',
      title: t('homepage.pillar4.title', 'Python-native'),
      body: t(
        'homepage.pillar4.body',
        'The SDK reads object storage directly through Rust IO. Daft, Ray, PyTorch and Pandas — no gateway in the path, no second copy of the data.'
      ),
    },
  ];

  const contrasts = [
    {
      title: t('homepage.fit.format.title', 'A table format'),
      body: t(
        'homepage.fit.format.body',
        'Only file layout. No catalog, no execution engine, no indexes, no incremental state.'
      ),
    },
    {
      title: t('homepage.fit.database.title', 'A database'),
      body: t(
        'homepage.fit.database.body',
        'Ships an engine, but the storage is its own. You move data in and operate its cluster.'
      ),
    },
  ];

  return (
    <Layout
      title={t('homepage.meta.title', 'An open lakehouse engine')}
      description={t(
        'homepage.meta.description',
        'LakeSoul is an open lakehouse engine: Postgres-native, vector-native and Python-native, in Rust, over your object storage.'
      )}>
      <Hero />

      <Section
        title={t('homepage.pillars.title', 'Four things it does')}
        subtitle={t(
          'homepage.pillars.subtitle',
          'One engine for SQL, incremental compute, search and AI — not four systems stitched together.'
        )}>
        <div className={styles.pillarGrid}>
          {pillars.map((pillar) => (
            <article className={styles.pillar} key={pillar.title}>
              <span className={styles.pillarTag}>{pillar.tag}</span>
              <h3 className={styles.pillarTitle}>{pillar.title}</h3>
              <p className={styles.pillarBody}>{pillar.body}</p>
            </article>
          ))}
        </div>
      </Section>

      <Section
        title={t('homepage.federation.title', 'Federated query')}
        subtitle={t(
          'homepage.federation.subtitle',
          'Query a remote database where it lives, join it with LakeSoul tables, and read or write it through the same PostgreSQL endpoint.'
        )}>
        <div className={styles.quickGrid}>
          <article className={styles.quickCard}>
            <h3 className={styles.quickTitle}>
              {t('homepage.federation.code.title', 'Define a foreign table')}
            </h3>
            <CodeBlock language="sql">{`CREATE EXTENSION adbc_fdw;

CREATE SERVER app_source
FOREIGN DATA WRAPPER adbc_fdw
OPTIONS (
  driver 'postgresql',
  uri 'postgresql://source:5432/app'
);

CREATE FOREIGN TABLE remote_orders (
  id bigint, region text, amount numeric(20,4)
)
SERVER app_source
OPTIONS (schema_name 'public', table_name 'orders');`}</CodeBlock>
            <p className={styles.quickNote}>
              <Link to="/docs/engine/postgres/federation">
                {t(
                  'homepage.federation.code.note',
                  'Foreign tables, pushdown and writes'
                )}
              </Link>
            </p>
          </article>

          <article className={styles.quickCard}>
            <h3 className={styles.quickTitle}>
              {t('homepage.federation.join.title', 'Join remote and local data')}
            </h3>
            <CodeBlock language="sql">{`SELECT u.id, u.name, sum(o.amount) AS total
FROM local_users u
JOIN remote_orders o USING (id)
GROUP BY 1, 2;`}</CodeBlock>
            <div className={styles.fitChips}>
              <span>PostgreSQL</span>
              <span>MySQL</span>
              <span>SQLite</span>
              <span>DuckDB</span>
              <span>BigQuery</span>
              <span>Snowflake</span>
              <span>Trino</span>
              <span>S3 files</span>
            </div>
          </article>
        </div>
      </Section>

      <Section
        tinted
        title={t('homepage.incremental.title', 'Incremental SQL')}
        subtitle={t(
          'homepage.incremental.subtitle',
          'Maintain a derived table from the source commit log. Each run reads only new commits and writes only the rows that changed; the result matches a full recompute.'
        )}>
        <div className={styles.quickGrid}>
          <article className={styles.quickCard}>
            <h3 className={styles.quickTitle}>
              {t('homepage.incremental.code.title', 'Run it as INSERT ... SELECT')}
            </h3>
            <CodeBlock language="sql">{`SET lakesoul.ivm.enabled = on;

INSERT INTO daily_revenue
SELECT date_trunc('day', ts) AS day,
       region,
       sum(amount)
FROM orders
GROUP BY 1, 2;`}</CodeBlock>
            <p className={styles.quickNote}>
              <Link to="/docs/engine/incremental-sql/overview">
                {t('homepage.incremental.code.note', 'How incremental SQL works')}
              </Link>
            </p>
          </article>

          <article className={styles.quickCard}>
            <h3 className={styles.quickTitle}>
              {t('homepage.incremental.state.title', 'Replay-safe state')}
            </h3>
            <p className={styles.pillarBody}>
              {t(
                'homepage.incremental.state.body',
                'The engine keeps a cursor per target, folds the source commit log into the query, and writes target deltas. Retries replay the same window and skip rows already written.'
              )}
            </p>
            <div className={styles.fitChips}>
              <span>{t('homepage.incremental.chipCursor', 'commit-log cursor')}</span>
              <span>{t('homepage.incremental.chipPartition', 'partition-aware')}</span>
              <span>{t('homepage.incremental.chipReplay', 'replay-safe')}</span>
              <span>
                {t('homepage.incremental.chipShapes', 'joins, aggregates, windows')}
              </span>
            </div>
          </article>
        </div>
      </Section>

      <Section
        title={t('homepage.subscription.title', 'PostgreSQL subscriptions')}
        subtitle={t(
          'homepage.subscription.subtitle',
          'Consume PostgreSQL or MySQL changes into a LakeSoul CDC table. The endpoint manages the snapshot, the changes and the subscriber process.'
        )}>
        <div className={styles.quickGrid}>
          <article className={styles.quickCard}>
            <h3 className={styles.quickTitle}>
              {t('homepage.subscription.code.title', 'Define it from psql')}
            </h3>
            <CodeBlock language="sql">{`CREATE SUBSCRIPTION users_from_app
CONNECTION 'postgresql://user:pass@source:5432/app'
PUBLICATION app_pub
WITH (
  source_table = 'public.users',
  target_table = 'users',
  snapshot     = 'managed'
);`}</CodeBlock>
            <p className={styles.quickNote}>
              <Link to="/docs/engine/postgres/subscription">
                {t(
                  'homepage.subscription.code.note',
                  'Set up the source publication and target table'
                )}
              </Link>
            </p>
          </article>

          <article className={styles.quickCard}>
            <h3 className={styles.quickTitle}>
              {t('homepage.subscription.managed.title', 'Managed end to end')}
            </h3>
            <p className={styles.pillarBody}>
              {t(
                'homepage.subscription.managed.body',
                'The endpoint validates the source and the CDC target, takes a managed snapshot with one atomic handoff, then streams changes in batches. ENABLE, DISABLE and DROP control the supervised local process.'
              )}
            </p>
            <div className={styles.fitChips}>
              <span>{t('homepage.subscription.chipSnapshot', 'managed snapshot')}</span>
              <span>{t('homepage.subscription.chipChanges', 'logical replication')}</span>
              <span>{t('homepage.subscription.chipAtomic', 'atomic handoff')}</span>
              <span>{t('homepage.subscription.chipLifecycle', 'supervised process')}</span>
            </div>
          </article>
        </div>
      </Section>

      <Section
        title={t('homepage.auth.title', 'Authentication and Permissions')}
        subtitle={t(
          'homepage.auth.subtitle',
          'Clients connect with a username and password. Each connection is scoped to the permissions granted to that user, so teams and agents see only the data they are allowed to use.'
        )}>
        <div className={styles.quickGrid}>
          <article className={styles.quickCard}>
            <h3 className={styles.quickTitle}>
              {t('homepage.auth.connect.title', 'Connect as a user')}
            </h3>
            <CodeBlock language="bash">{`export PGPASSWORD=******
psql -h lakesoul.example.com \
  -p 5432 -U alice -d default`}</CodeBlock>
            <p className={styles.quickNote}>
              <Link to="/docs/engine/postgres/authentication">
                {t(
                  'homepage.auth.connect.note',
                  'Authentication and Permissions'
                )}
              </Link>
            </p>
          </article>

          <article className={styles.quickCard}>
            <h3 className={styles.quickTitle}>
              {t('homepage.auth.scope.title', 'Scope each workload')}
            </h3>
            <p className={styles.pillarBody}>
              {t(
                'homepage.auth.scope.body',
                'Run separate compute clusters for different teams or agents against the same catalog. A connection sees only the namespaces and tables granted to its user.'
              )}
            </p>
            <div className={styles.fitChips}>
              <span>{t('homepage.auth.chipUser', 'username / password')}</span>
              <span>
                {t('homepage.auth.chipPermissions', 'per-user permissions')}
              </span>
              <span>{t('homepage.auth.chipIsolation', 'workload isolation')}</span>
            </div>
          </article>
        </div>
      </Section>

      <Section
        tinted
        title={t('homepage.fit.title', 'How it differs')}
        subtitle={t(
          'homepage.fit.subtitle',
          'A table format only describes file layout. A database ships an engine but owns your storage. LakeSoul is neither: it is the whole engine, and your data stays in your object storage.'
        )}>
        <div className={styles.fitGrid}>
          {contrasts.map((contrast) => (
            <article className={styles.fitCard} key={contrast.title}>
              <h3 className={styles.fitTitle}>{contrast.title}</h3>
              <p className={styles.fitBody}>{contrast.body}</p>
            </article>
          ))}
        </div>

        <div className={styles.fitCallout}>
          <span className={styles.fitCalloutBadge}>LakeSoul</span>
          <h3 className={styles.fitCalloutTitle}>
            {t(
              'homepage.fit.lakesoul.title',
              'The catalog, the format, the engine and the indexes — storage stays yours.'
            )}
          </h3>
          <p className={styles.fitCalloutBody}>
            {t(
              'homepage.fit.lakesoul.body',
              'Your data stays in your object storage, in open files. What you run on top is the whole engine, not just a compute layer.'
            )}
          </p>
          <div className={styles.fitChips}>
            <span>{t('homepage.fit.chipCatalog', 'catalog')}</span>
            <span>{t('homepage.fit.chipEngine', 'execution engine')}</span>
            <span>{t('homepage.fit.chipIndex', 'vector + full-text indexes')}</span>
            <span>{t('homepage.fit.chipStorage', 'your object storage')}</span>
          </div>
        </div>
      </Section>

      <Section
        title={t('homepage.arch.title', 'Architecture')}
        subtitle={t(
          'homepage.arch.subtitle',
          'Clients on top, multiple compute clusters in the middle, your storage at the bottom.'
        )}>
        <div className={styles.arch}>
          <div className={styles.archRow}>
            <span className={styles.archBox}>
              {t('homepage.arch.clientsSql', 'psycopg · JDBC · RAG')}
            </span>
            <span className={styles.archBox}>
              {t('homepage.arch.clientsPython', 'Python SDK · Daft · Ray · PyTorch')}
            </span>
          </div>
          <div className={styles.archArrow} aria-hidden="true">
            ↓
          </div>
          <div className={styles.archClusters}>
            <article className={styles.archCluster}>
              <strong>{t('homepage.arch.clusterA', 'Compute cluster A')}</strong>
              <span>
                {t(
                  'homepage.arch.clusterAWorkload',
                  'Subscriptions · FDW'
                )}
              </span>
            </article>
            <article className={styles.archCluster}>
              <strong>{t('homepage.arch.clusterB', 'Compute cluster B')}</strong>
              <span>
                {t(
                  'homepage.arch.clusterBWorkload',
                  'Incremental SQL'
                )}
              </span>
            </article>
            <article className={styles.archCluster}>
              <strong>{t('homepage.arch.clusterC', 'Compute cluster C')}</strong>
              <span>
                {t(
                  'homepage.arch.clusterCWorkload',
                  'BI Query · ANN'
                )}
              </span>
            </article>
          </div>
          <div className={styles.archArrow} aria-hidden="true">
            ↓
          </div>
          <div className={styles.archRow}>
            <span className={styles.archBox}>
              {t('homepage.arch.storage', 'Object storage (S3 / OSS / HDFS)')}
            </span>
            <span className={styles.archBox}>
              {t('homepage.arch.metadata', 'PostgreSQL metadata')}
            </span>
          </div>
        </div>
      </Section>

      <Section
        tinted
        title={t('homepage.quickstart.title', 'Run it')}
        subtitle={t(
          'homepage.quickstart.subtitle',
          'Start the PostgreSQL endpoint, or install the Python SDK and read the same tables.'
        )}>
        <div className={styles.quickGrid}>
          <article className={styles.quickCard}>
            <h3 className={styles.quickTitle}>
              {t('homepage.quickstart.pg.title', 'PostgreSQL endpoint')}
            </h3>
            <CodeBlock language="bash">{`cargo run -p postgres-lakesoul -- --port 5433 \\
  --warehouse-prefix file:///tmp/lakesoul/warehouse

psql -h 127.0.0.1 -p 5433 -U lakesoul_test -d default`}</CodeBlock>
            <p className={styles.quickNote}>
              <Link to={QUICKSTART_URL}>
                {t('homepage.quickstart.pg.note', 'Full setup: metadata database and endpoint')}
              </Link>
            </p>
          </article>

          <article className={styles.quickCard}>
            <h3 className={styles.quickTitle}>
              {t('homepage.quickstart.python.title', 'Python SDK')}
            </h3>
            <CodeBlock language="python">{`from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
scan = catalog.scan("orders").select("id", "amount")
table = scan.to_arrow_table()`}</CodeBlock>
            <p className={styles.quickNote}>
              <Link to={QUICKSTART_URL}>
                {t('homepage.quickstart.python.note', 'Python quick start')}
              </Link>
            </p>
          </article>
        </div>
      </Section>

      <section className={styles.community}>
        <div className={styles.sectionInner}>
          <h2 className={styles.communityTitle}>
            {t('homepage.community.title', 'Build with us')}
          </h2>
          <p className={styles.communityBody}>
            {t(
              'homepage.community.body',
              'LakeSoul is an open source project under LF AI & Data. The Rust engine, the PostgreSQL front end and the Python SDK are all in the same repository.'
            )}
          </p>
          <div className={styles.communityLinks}>
            <Link className={styles.communityLink} href={GITHUB_URL}>
              GitHub
            </Link>
            <Link
              className={styles.communityLink}
              href={`${GITHUB_URL}/labels/good%20first%20issue`}>
              {t('homepage.community.issues', 'Good first issues')}
            </Link>
            <Link className={styles.communityLink} href="https://discord.gg/WJrHKq4BPf">
              Discord
            </Link>
            <Link className={styles.communityLink} href={`${GITHUB_URL}/blob/main/ROADMAP.md`}>
              {t('homepage.community.roadmap', 'Roadmap')}
            </Link>
          </div>
        </div>
      </section>
    </Layout>
  );
}
