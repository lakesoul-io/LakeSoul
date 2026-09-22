// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

// Command weknora-es-harness replays the Elasticsearch request sequence of
// WeKnora's v8 retriever driver against a LakeSoul ES gateway.
//
// It uses the same client library and the same request shapes as
// Tencent/WeKnora (internal/application/repository/retriever/elasticsearch/v8)
// so that wire-level incompatibilities surface without deploying the full
// WeKnora stack.  Embeddings are synthetic unit vectors; no embedding API is
// needed.  Run it again with --client=v7 once the v8 path is green.
//
// Usage:
//
//	go run ./script/weknora-es-harness \
//	    --addr http://127.0.0.1:9200 --index WeKnora --dim 1024
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/scriptlanguage"
)

type config struct {
	addr   string
	index  string
	dim    int
	docs   int
	client string
}

type harness struct {
	cfg    config
	client *elasticsearch.TypedClient
	run    string // unique suffix isolating this run's documents
	passed int
	failed int
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.addr, "addr", "http://127.0.0.1:9200", "gateway address")
	flag.StringVar(&cfg.index, "index", "WeKnora", "index name (ELASTICSEARCH_INDEX)")
	flag.IntVar(&cfg.dim, "dim", 1024, "embedding dimension configured on the gateway")
	flag.IntVar(&cfg.docs, "docs", 20, "documents written per knowledge base")
	flag.StringVar(&cfg.client, "client", "v8", "client to replay: v8 (typed) or v7 (esapi)")
	flag.Parse()

	if cfg.client == "v7" {
		os.Exit(runV7(cfg))
	}

	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{cfg.addr},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "create client: %v\n", err)
		os.Exit(2)
	}
	h := &harness{
		cfg:    cfg,
		client: client,
		run:    fmt.Sprintf("harness-%d", time.Now().UnixNano()),
	}
	h.runAll()
	fmt.Printf("\n%d passed, %d failed\n", h.passed, h.failed)
	if h.failed > 0 {
		os.Exit(1)
	}
}

func (h *harness) runAll() {
	ctx := context.Background()
	h.step("client info (version/product header)", func() error {
		info, err := h.client.Info().Do(ctx)
		if err != nil {
			return err
		}
		version := info.Version.Int
		if !strings.HasPrefix(version, "8.") {
			return fmt.Errorf("unexpected version %q", version)
		}
		fmt.Printf("    version %s\n", version)
		return nil
	})

	h.step("indices.exists + create if missing", func() error {
		exists, err := h.client.Indices.Exists(h.cfg.index).Do(ctx)
		if err != nil {
			return err
		}
		if exists {
			fmt.Printf("    exists=true (gateway pre-provisions; create skipped)\n")
			return nil
		}
		shards, replicas := "4", "0"
		_, err = h.client.Indices.Create(h.cfg.index).
			Settings(&types.IndexSettings{
				NumberOfShards:   &shards,
				NumberOfReplicas: &replicas,
			}).
			Do(ctx)
		return err
	})

	h.step("indices.get_mapping + .keyword detection", func() error {
		resp, err := h.client.Indices.GetMapping().Index(h.cfg.index).Do(ctx)
		if err != nil {
			return err
		}
		mapping, ok := resp[h.cfg.index]
		if !ok {
			return fmt.Errorf("index %q missing from mapping response", h.cfg.index)
		}
		prop, ok := mapping.Mappings.Properties["chunk_id"]
		if !ok {
			return fmt.Errorf("chunk_id missing from mapping properties")
		}
		raw, err := json.Marshal(prop)
		if err != nil {
			return err
		}
		var info struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &info); err != nil {
			return err
		}
		if info.Type != "keyword" {
			return fmt.Errorf("chunk_id maps to %q; WeKnora would switch to the .keyword suffix", info.Type)
		}
		fmt.Printf("    chunk_id=keyword (bare field names)\n")
		return nil
	})

	kbA := h.run + "-kb-a"
	kbB := h.run + "-kb-b"
	sourceID := h.run + "-src"
	docs := makeDocuments(h.cfg, h.run)
	_ = sourceID
	chunkIDs := make([]string, 0, len(docs))

	h.step("index document (chunk edit path)", func() error {
		_, err := h.client.Index(h.cfg.index).Request(docs[0]).Do(ctx)
		return err
	})
	chunkIDs = append(chunkIDs, docs[0].ChunkID)

	h.step("bulk create", func() error {
		req := h.client.Bulk().Index(h.cfg.index)
		for _, doc := range docs[1:] {
			if err := req.CreateOp(types.CreateOperation{Index_: &h.cfg.index}, doc); err != nil {
				return err
			}
		}
		resp, err := req.Do(ctx)
		if err != nil {
			return err
		}
		if resp.Errors {
			return fmt.Errorf("bulk reported errors: %+v", resp.Items[:min(2, len(resp.Items))])
		}
		for _, doc := range docs[1:] {
			chunkIDs = append(chunkIDs, doc.ChunkID)
		}
		return nil
	})

	h.step("keywords retrieve (bool filter + match)", func() error {
		filter := h.baseConds(kbA, false)
		must := []types.Query{{Match: map[string]types.MatchQuery{"content": {Query: "zebra"}}}}
		resp, err := h.client.Search().Index(h.cfg.index).Request(&search.Request{
			Query:   &types.Query{Bool: &types.BoolQuery{Filter: filter, Must: must}},
			Size:    ptr(10),
			Source_: &types.SourceFilter{Excludes: []string{"embedding"}},
		}).Do(ctx)
		if err != nil {
			return err
		}
		if len(resp.Hits.Hits) != 1 {
			return fmt.Errorf("expected the zebra document, got %d hits", len(resp.Hits.Hits))
		}
		hit := resp.Hits.Hits[0]
		if hit.Score_ == nil || *hit.Score_ <= 0 {
			return fmt.Errorf("hit without a positive _score")
		}
		var doc VectorEmbedding
		if err := json.Unmarshal(hit.Source_, &doc); err != nil {
			return fmt.Errorf("unmarshal _source: %w", err)
		}
		if doc.ChunkID == "" || len(doc.Embedding) != 0 {
			return fmt.Errorf("_source missing chunk_id or includes embedding: %+v", doc)
		}
		fmt.Printf("    hit chunk_id=%s score=%.4f\n", doc.ChunkID, *hit.Score_)
		return nil
	})

	h.step("vector retrieve (script_score + min_score)", func() error {
		queryVector := docs[3].Embedding
		raw, err := json.Marshal(queryVector)
		if err != nil {
			return err
		}
		source := "Math.max(cosineSimilarity(params.query_vector, 'embedding'), 0.0)"
		lang := scriptlanguage.Painless
		minScore := float32(0.9)
		resp, err := h.client.Search().Index(h.cfg.index).Request(&search.Request{
			Query: &types.Query{ScriptScore: &types.ScriptScoreQuery{
				Query: types.Query{Bool: &types.BoolQuery{Filter: h.baseConds(kbA, false)}},
				Script: types.Script{
					Source: &source,
					Lang:   &lang,
					Params: map[string]json.RawMessage{"query_vector": raw},
				},
				MinScore: &minScore,
			}},
			Size:    ptr(5),
			Source_: &types.SourceFilter{Excludes: []string{"embedding"}},
		}).Do(ctx)
		if err != nil {
			return err
		}
		if len(resp.Hits.Hits) == 0 {
			return fmt.Errorf("no vector hits above min_score")
		}
		var got VectorEmbedding
		if err := json.Unmarshal(resp.Hits.Hits[0].Source_, &got); err != nil {
			return err
		}
		if got.ChunkID != docs[3].ChunkID {
			return fmt.Errorf("top vector hit is %s, want %s", got.ChunkID, docs[3].ChunkID)
		}
		fmt.Printf("    top chunk_id=%s score=%.4f\n", got.ChunkID, *resp.Hits.Hits[0].Score_)
		return nil
	})

	h.step("update_by_query: disable + enable", func() error {
		if err := h.updateScript(chunkIDs[:3], "ctx._source.is_enabled = false", nil, false); err != nil {
			return err
		}
		resp, err := h.keywordHits(kbA)
		if err != nil {
			return err
		}
		for _, hit := range resp {
			if hit.ChunkID == chunkIDs[0] {
				return fmt.Errorf("disabled chunk %s still returned", hit.ChunkID)
			}
		}
		return h.updateScript(chunkIDs[:3], "ctx._source.is_enabled = true", nil, false)
	})

	h.step("update_by_query: tag_id", func() error {
		return h.updateScript(chunkIDs[:2], "ctx._source.tag_id = params.tag_id",
			map[string]json.RawMessage{"tag_id": json.RawMessage(`"tag-smoke"`)}, false)
	})

	h.step("update_by_query: move knowledge base", func() error {
		if err := h.updateScript(chunkIDs[:2],
			"ctx._source.knowledge_base_id = params.target; ctx._source.tag_id = ''",
			map[string]json.RawMessage{"target": json.RawMessage(fmt.Sprintf("%q", kbB))}, true); err != nil {
			return err
		}
		hitsA, err := h.keywordHits(kbA)
		if err != nil {
			return err
		}
		for _, hit := range hitsA {
			if hit.ChunkID == chunkIDs[0] || hit.ChunkID == chunkIDs[1] {
				return fmt.Errorf("moved chunk %s still in source knowledge base", hit.ChunkID)
			}
		}
		hitsB, err := h.keywordHits(kbB)
		if err != nil {
			return err
		}
		if len(hitsB) != 2 {
			return fmt.Errorf("moved knowledge base has %d chunks, want 2", len(hitsB))
		}
		return nil
	})

	h.step("copy indices (filter-only from/size paging)", func() error {
		copied := 0
		for from := 0; ; from += 5 {
			fromCopy := from
			resp, err := h.client.Search().Index(h.cfg.index).Request(&search.Request{
				Query: &types.Query{Bool: &types.BoolQuery{Filter: h.baseConds(kbA, false)}},
				From:  &fromCopy,
				Size:  ptr(5),
			}).Do(ctx)
			if err != nil {
				return err
			}
			if len(resp.Hits.Hits) == 0 {
				break
			}
			req := h.client.Bulk().Index(h.cfg.index)
			for _, hit := range resp.Hits.Hits {
				var doc VectorEmbedding
				if err := json.Unmarshal(hit.Source_, &doc); err != nil {
					return err
				}
				doc.ChunkID += "-copy"
				doc.KnowledgeBaseID = kbB
				if err := req.CreateOp(types.CreateOperation{Index_: &h.cfg.index}, doc); err != nil {
					return err
				}
				copied++
			}
			if _, err := req.Do(ctx); err != nil {
				return err
			}
			fmt.Printf("    from=%d page=%d\n", from, len(resp.Hits.Hits))
			if copied > 100 {
				return fmt.Errorf("paging did not terminate")
			}
		}
		if copied == 0 {
			return fmt.Errorf("no documents paged from the source knowledge base")
		}
		hitsB, err := h.keywordHits(kbB)
		if err != nil {
			return err
		}
		fmt.Printf("    copied %d documents, destination now has %d chunks\n", copied, len(hitsB))
		return nil
	})

	h.step("delete_by_query (terms chunk_id)", func() error {
		_, err := h.client.DeleteByQuery(h.cfg.index).Query(&types.Query{
			Terms: &types.TermsQuery{TermsQuery: map[string]types.TermsQueryField{
				"chunk_id": chunkIDs,
			}},
		}).Do(ctx)
		return err
	})
}

// makeDocuments builds documents in WeKnora's VectorEmbedding shape.  One
// document carries a unique keyword ("zebra") for the keyword-retrieval
// assertion; every embedding is a deterministic unit vector.
func makeDocuments(cfg config, run string) []VectorEmbedding {
	docs := make([]VectorEmbedding, 0, cfg.docs)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < cfg.docs; i++ {
		vector := make([]float32, cfg.dim)
		var norm float64
		for j := range vector {
			vector[j] = float32(rng.NormFloat64())
			norm += float64(vector[j]) * float64(vector[j])
		}
		norm = math.Sqrt(norm)
		for j := range vector {
			vector[j] = float32(float64(vector[j]) / norm)
		}
		content := fmt.Sprintf("harness document %d about lakehouse text search", i)
		if i == 5 {
			content = "the unique zebra harness document for keyword retrieval"
		}
		sourceID := run + "-src"
		docs = append(docs, VectorEmbedding{
			Content:         content,
			SourceID:        sourceID,
			SourceType:      1,
			ChunkID:         fmt.Sprintf("%s-chunk-%02d", run, i),
			KnowledgeID:     sourceID,
			KnowledgeBaseID: run + "-kb-a",
			TagID:           "",
			Embedding:       vector,
			IsEnabled:       true,
			IsRecommended:   false,
		})
	}
	return docs
}

// baseConds mirrors WeKnora's getBaseConds: a bool query with terms filters
// and must_not is_enabled=false, nested inside the caller's filter array.
func (h *harness) baseConds(kb string, forMove bool) []types.Query {
	must := []types.Query{{Terms: &types.TermsQuery{
		TermsQuery: map[string]types.TermsQueryField{"knowledge_base_id": []string{kb}},
	}}}
	mustNot := []types.Query{{Term: map[string]types.TermQuery{
		"is_enabled": {Value: false},
	}}}
	return []types.Query{{Bool: &types.BoolQuery{Must: must, MustNot: mustNot}}}
}

// updateScript sends one of the four fixed Painless scripts WeKnora uses,
// asserting the response fields its MoveKnowledgeIndices validates.
func (h *harness) updateScript(chunkIDs []string, source string,
	params map[string]json.RawMessage, refresh bool,
) error {
	lang := scriptlanguage.Painless
	query := &types.Query{Bool: &types.BoolQuery{Must: []types.Query{
		{Terms: &types.TermsQuery{TermsQuery: map[string]types.TermsQueryField{
			"chunk_id": chunkIDs,
		}}},
	}}}
	req := h.client.UpdateByQuery(h.cfg.index).Query(query).Script(&types.Script{
		Source: &source,
		Lang:   &lang,
		Params: params,
	})
	ctx := context.Background()
	if refresh {
		req = req.Refresh(true)
	}
	resp, err := req.Do(ctx)
	if err != nil {
		return err
	}
	if resp.Total == nil || resp.Updated == nil {
		return fmt.Errorf("response without total/updated: %+v", resp)
	}
	if *resp.Total != *resp.Updated {
		return fmt.Errorf("total=%d updated=%d", *resp.Total, *resp.Updated)
	}
	if resp.TimedOut != nil && *resp.TimedOut {
		return fmt.Errorf("update timed out")
	}
	if resp.VersionConflicts != nil && *resp.VersionConflicts != 0 {
		return fmt.Errorf("version conflicts: %d", *resp.VersionConflicts)
	}
	if len(resp.Failures) != 0 {
		return fmt.Errorf("failures: %+v", resp.Failures)
	}
	return nil
}

// keywordHits runs the keyword retrieval and returns the documents ordered by
// descending score.
func (h *harness) keywordHits(kb string) ([]VectorEmbedding, error) {
	resp, err := h.client.Search().Index(h.cfg.index).Request(&search.Request{
		Query: &types.Query{Bool: &types.BoolQuery{
			Filter: h.baseConds(kb, false),
			Must:   []types.Query{{Match: map[string]types.MatchQuery{"content": {Query: "harness"}}}},
		}},
		Size: ptr(100),
	}).Do(context.Background())
	if err != nil {
		return nil, err
	}
	docs := make([]VectorEmbedding, 0, len(resp.Hits.Hits))
	scores := make([]float64, 0, len(resp.Hits.Hits))
	for _, hit := range resp.Hits.Hits {
		var doc VectorEmbedding
		if err := json.Unmarshal(hit.Source_, &doc); err != nil {
			return nil, err
		}
		docs = append(docs, doc)
		if hit.Score_ == nil {
			return nil, fmt.Errorf("hit %s has no _score", doc.ChunkID)
		}
		scores = append(scores, float64(*hit.Score_))
	}
	if !sort.SliceIsSorted(scores, func(i, j int) bool { return scores[i] > scores[j] }) {
		return nil, fmt.Errorf("hits are not in descending score order: %v", scores)
	}
	return docs, nil
}

func (h *harness) step(name string, fn func() error) {
	fmt.Printf("── %s\n", name)
	if err := fn(); err != nil {
		h.failed++
		fmt.Printf("   FAIL: %v\n", err)
		return
	}
	h.passed++
	fmt.Printf("   PASS\n")
}

// VectorEmbedding mirrors WeKnora's Elasticsearch document (JSON tags from
// internal/application/repository/retriever/elasticsearch/structs.go).
type VectorEmbedding struct {
	Content         string    `json:"content"`
	SourceID        string    `json:"source_id"`
	SourceType      int       `json:"source_type"`
	ChunkID         string    `json:"chunk_id"`
	KnowledgeID     string    `json:"knowledge_id"`
	KnowledgeBaseID string    `json:"knowledge_base_id"`
	TagID           string    `json:"tag_id"`
	Embedding       []float32 `json:"embedding"`
	IsEnabled       bool      `json:"is_enabled"`
	IsRecommended   bool      `json:"is_recommended"`
}

func ptr[T any](value T) *T { return &value }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
