// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

// v7 replay: WeKnora's v7 retriever driver uses the low-level esapi client
// (github.com/elastic/go-elasticsearch/v7), so the same request shapes are
// sent through that client instead of the v8 typed client.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type harnessV7 struct {
	cfg    config
	client *elasticsearch7.Client
	run    string
	passed int
	failed int
}

func runV7(cfg config) int {
	client, err := elasticsearch7.NewClient(elasticsearch7.Config{
		Addresses: []string{cfg.addr},
	})
	if err != nil {
		fmt.Printf("create client: %v\n", err)
		return 2
	}
	h := &harnessV7{
		cfg:    cfg,
		client: client,
		run:    fmt.Sprintf("harness-v7-%d", time.Now().UnixNano()),
	}
	h.runAll()
	fmt.Printf("\n%d passed, %d failed\n", h.passed, h.failed)
	if h.failed > 0 {
		return 1
	}
	return 0
}

func (h *harnessV7) runAll() {
	ctx := context.Background()
	kbA := h.run + "-kb-a"
	kbB := h.run + "-kb-b"
	docs := makeDocuments(h.cfg, h.run)
	for i := range docs {
		docs[i].KnowledgeBaseID = kbA
	}
	chunkIDs := make([]string, 0, len(docs))

	h.step("client info (version/product header)", func() error {
		body, err := h.do(ctx, func() (*esapi.Response, error) { return h.client.Info() })
		if err != nil {
			return err
		}
		version := nestedString(body, "version", "number")
		if !strings.HasPrefix(version, "8.") {
			return fmt.Errorf("unexpected version %q", version)
		}
		fmt.Printf("    version %s\n", version)
		return nil
	})

	h.step("indices.exists", func() error {
		if _, err := h.do(ctx, func() (*esapi.Response, error) {
			return h.client.Indices.Exists([]string{h.cfg.index})
		}); err != nil {
			return err
		}
		fmt.Printf("    exists (the gateway pre-provisions on start)\n")
		return nil
	})

	h.step("indices.get_mapping + .keyword detection", func() error {
		body, err := h.do(ctx, func() (*esapi.Response, error) {
			return h.client.Indices.GetMapping(
				h.client.Indices.GetMapping.WithIndex(h.cfg.index))
		})
		if err != nil {
			return err
		}
		index, ok := body[h.cfg.index].(map[string]any)
		if !ok {
			return fmt.Errorf("index %q missing from mapping response", h.cfg.index)
		}
		kind := nestedString(index, "mappings", "properties", "chunk_id", "type")
		if kind != "keyword" {
			return fmt.Errorf("chunk_id maps to %q; driver would use .keyword", kind)
		}
		fmt.Printf("    chunk_id=keyword (bare field names)\n")
		return nil
	})

	h.step("index document (chunk edit path)", func() error {
		_, err := h.do(ctx, func() (*esapi.Response, error) {
			return h.client.Index(h.cfg.index, jsonReader(docs[0]))
		})
		return err
	})
	chunkIDs = append(chunkIDs, docs[0].ChunkID)

	h.step("bulk create", func() error {
		var buffer bytes.Buffer
		for _, doc := range docs[1:] {
			buffer.WriteString(fmt.Sprintf("{\"create\":{\"_index\":%q}}\n", h.cfg.index))
			raw, _ := json.Marshal(doc)
			buffer.Write(raw)
			buffer.WriteString("\n")
		}
		body, err := h.do(ctx, func() (*esapi.Response, error) {
			return h.client.Bulk(bytes.NewReader(buffer.Bytes()),
				h.client.Bulk.WithIndex(h.cfg.index))
		})
		if err != nil {
			return err
		}
		if errors, ok := body["errors"].(bool); ok && errors {
			return fmt.Errorf("bulk reported errors")
		}
		for _, doc := range docs[1:] {
			chunkIDs = append(chunkIDs, doc.ChunkID)
		}
		return nil
	})

	h.step("keywords retrieve (bool filter + match)", func() error {
		query := map[string]any{
			"query": map[string]any{"bool": map[string]any{
				"filter": h.baseConds(kbA),
				"must": []any{map[string]any{
					"match": map[string]any{"content": map[string]any{"query": "zebra"}},
				}},
			}},
			"size":    10,
			"_source": map[string]any{"excludes": []string{"embedding"}},
		}
		body, err := h.search(ctx, query)
		if err != nil {
			return err
		}
		hits := hitsOf(body)
		if len(hits) != 1 {
			return fmt.Errorf("expected the zebra document, got %d hits", len(hits))
		}
		score, _ := hits[0]["_score"].(float64)
		if score <= 0 {
			return fmt.Errorf("hit without a positive _score")
		}
		doc := sourceOf(hits[0])
		if doc.ChunkID == "" || len(doc.Embedding) != 0 {
			return fmt.Errorf("_source missing chunk_id or includes embedding")
		}
		fmt.Printf("    hit chunk_id=%s score=%.4f\n", doc.ChunkID, score)
		return nil
	})

	h.step("vector retrieve (script_score + min_score)", func() error {
		query := map[string]any{
			"query": map[string]any{"script_score": map[string]any{
				"query": map[string]any{"bool": map[string]any{"filter": h.baseConds(kbA)}},
				"script": map[string]any{
					"source": "cosineSimilarity(params.query_vector, 'embedding')",
					"params": map[string]any{"query_vector": docs[3].Embedding},
				},
				"min_score": 0.9,
			}},
			"size":    5,
			"_source": map[string]any{"excludes": []string{"embedding"}},
		}
		body, err := h.search(ctx, query)
		if err != nil {
			return err
		}
		hits := hitsOf(body)
		if len(hits) == 0 {
			return fmt.Errorf("no vector hits above min_score")
		}
		doc := sourceOf(hits[0])
		if doc.ChunkID != docs[3].ChunkID {
			return fmt.Errorf("top vector hit is %s, want %s", doc.ChunkID, docs[3].ChunkID)
		}
		return nil
	})

	h.step("update_by_query: disable + enable", func() error {
		if err := h.updateScript(ctx, chunkIDs[:3], "ctx._source.is_enabled = false", nil, false); err != nil {
			return err
		}
		disabled, err := h.keywordDocs(ctx, kbA)
		if err != nil {
			return err
		}
		for _, doc := range disabled {
			if doc.ChunkID == chunkIDs[0] {
				return fmt.Errorf("disabled chunk %s still returned", doc.ChunkID)
			}
		}
		return h.updateScript(ctx, chunkIDs[:3], "ctx._source.is_enabled = true", nil, false)
	})
	h.step("update_by_query: tag_id", func() error {
		return h.updateScript(ctx, chunkIDs[:2], "ctx._source.tag_id = params.tag_id",
			map[string]any{"tag_id": "tag-smoke"}, false)
	})
	h.step("update_by_query: move knowledge base", func() error {
		if err := h.updateScript(ctx, chunkIDs[:2],
			"ctx._source.knowledge_base_id = params.target; ctx._source.tag_id = ''",
			map[string]any{"target": kbB}, true); err != nil {
			return err
		}
		hits, err := h.keywordDocs(ctx, kbB)
		if err != nil {
			return err
		}
		if len(hits) != 2 {
			return fmt.Errorf("moved knowledge base has %d chunks, want 2", len(hits))
		}
		source, err := h.keywordDocs(ctx, kbA)
		if err != nil {
			return err
		}
		for _, doc := range source {
			if doc.ChunkID == chunkIDs[0] || doc.ChunkID == chunkIDs[1] {
				return fmt.Errorf("moved chunk %s still in source", doc.ChunkID)
			}
		}
		return nil
	})

	h.step("copy indices (filter-only from/size paging)", func() error {
		copied := 0
		for from := 0; copied <= 100; from += 5 {
			query := map[string]any{
				"query": map[string]any{"bool": map[string]any{"filter": h.baseConds(kbA)}},
				"from":  from,
				"size":  5,
			}
			body, err := h.search(ctx, query)
			if err != nil {
				return err
			}
			hits := hitsOf(body)
			if len(hits) == 0 {
				break
			}
			var buffer bytes.Buffer
			for _, hit := range hits {
				doc := sourceOf(hit)
				doc.ChunkID += "-copy"
				doc.KnowledgeBaseID = kbB
				buffer.WriteString(fmt.Sprintf("{\"create\":{\"_index\":%q}}\n", h.cfg.index))
				raw, _ := json.Marshal(doc)
				buffer.Write(raw)
				buffer.WriteString("\n")
				copied++
			}
			if _, err := h.do(ctx, func() (*esapi.Response, error) {
				return h.client.Bulk(bytes.NewReader(buffer.Bytes()),
					h.client.Bulk.WithIndex(h.cfg.index))
			}); err != nil {
				return err
			}
			fmt.Printf("    from=%d page=%d\n", from, len(hits))
		}
		if copied == 0 {
			return fmt.Errorf("no documents paged from the source knowledge base")
		}
		return nil
	})

	h.step("delete_by_query (terms chunk_id)", func() error {
		query := map[string]any{"query": map[string]any{
			"terms": map[string]any{"chunk_id": chunkIDs},
		}}
		_, err := h.do(ctx, func() (*esapi.Response, error) {
			return h.client.DeleteByQuery([]string{h.cfg.index}, jsonReader(query))
		})
		return err
	})
}

func (h *harnessV7) search(ctx context.Context, query map[string]any) (map[string]any, error) {
	return h.do(ctx, func() (*esapi.Response, error) {
		return h.client.Search(
			h.client.Search.WithIndex(h.cfg.index),
			h.client.Search.WithBody(jsonReader(query)),
		)
	})
}

func (h *harnessV7) baseConds(kb string) []any {
	return []any{map[string]any{"bool": map[string]any{
		"must": []any{map[string]any{"terms": map[string]any{
			"knowledge_base_id": []string{kb},
		}}},
		"must_not": []any{map[string]any{"term": map[string]any{
			"is_enabled": false,
		}}},
	}}}
}

func (h *harnessV7) updateScript(ctx context.Context, chunkIDs []string, source string,
	params map[string]any, refresh bool,
) error {
	script := map[string]any{"source": source}
	if params != nil {
		script["params"] = params
	}
	body := map[string]any{
		"query": map[string]any{"bool": map[string]any{"must": []any{
			map[string]any{"terms": map[string]any{"chunk_id": chunkIDs}},
		}}},
		"script": script,
	}
	response, err := h.do(ctx, func() (*esapi.Response, error) {
		options := []func(*esapi.UpdateByQueryRequest){
			h.client.UpdateByQuery.WithBody(jsonReader(body)),
		}
		if refresh {
			options = append(options, h.client.UpdateByQuery.WithRefresh(true))
		}
		return h.client.UpdateByQuery([]string{h.cfg.index}, options...)
	})
	if err != nil {
		return err
	}
	total, _ := response["total"].(float64)
	updated, _ := response["updated"].(float64)
	if total != updated {
		return fmt.Errorf("total=%v updated=%v", total, updated)
	}
	if conflicts, ok := response["version_conflicts"].(float64); ok && conflicts != 0 {
		return fmt.Errorf("version conflicts: %v", conflicts)
	}
	if failures, ok := response["failures"].([]any); ok && len(failures) != 0 {
		return fmt.Errorf("failures: %v", failures)
	}
	return nil
}

func (h *harnessV7) keywordDocs(ctx context.Context, kb string) ([]VectorEmbedding, error) {
	query := map[string]any{
		"query": map[string]any{"bool": map[string]any{
			"filter": h.baseConds(kb),
			"must": []any{map[string]any{
				"match": map[string]any{"content": map[string]any{"query": "harness"}},
			}},
		}},
		"size": 100,
	}
	body, err := h.search(ctx, query)
	if err != nil {
		return nil, err
	}
	docs := make([]VectorEmbedding, 0)
	previous := float64(1 << 30)
	for _, hit := range hitsOf(body) {
		score, _ := hit["_score"].(float64)
		if score > previous {
			return nil, fmt.Errorf("hits are not in descending score order")
		}
		previous = score
		docs = append(docs, sourceOf(hit))
	}
	return docs, nil
}

func (h *harnessV7) step(name string, fn func() error) {
	fmt.Printf("── %s\n", name)
	if err := fn(); err != nil {
		h.failed++
		fmt.Printf("   FAIL: %v\n", err)
		return
	}
	h.passed++
	fmt.Printf("   PASS\n")
}

// do executes an esapi request and decodes a JSON object response, failing on
// non-2xx status codes.
func (h *harnessV7) do(ctx context.Context, call func() (*esapi.Response, error)) (map[string]any, error) {
	response, err := call()
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.IsError() {
		return nil, fmt.Errorf("status %s: %s", response.Status(), truncate(string(raw), 200))
	}
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return body, nil
}

func jsonReader(value any) *bytes.Reader {
	raw, _ := json.Marshal(value)
	return bytes.NewReader(raw)
}

func hitsOf(body map[string]any) []map[string]any {
	hits, _ := body["hits"].(map[string]any)
	items, _ := hits["hits"].([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if hit, ok := item.(map[string]any); ok {
			out = append(out, hit)
		}
	}
	return out
}

func sourceOf(hit map[string]any) VectorEmbedding {
	var doc VectorEmbedding
	source, _ := hit["_source"].(map[string]any)
	raw, _ := json.Marshal(source)
	_ = json.Unmarshal(raw, &doc)
	return doc
}

func nestedString(node map[string]any, keys ...string) string {
	var current any = node
	for _, key := range keys {
		object, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = object[key]
	}
	value, _ := current.(string)
	return value
}

func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit]
}
