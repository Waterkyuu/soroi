import test from "node:test";
import assert from "node:assert/strict";
import { chapters, prepareMarkdown } from "./prepare-docs.mts";

test("both languages retain code and resolve repository links for the website", () => {
	const source =
		"# Guide\n\n[English](queries.en-US.md) · [缓存](cache.zh-CN.md)\n\n[Cache](cache.en-US.md#expiry)\n[Test](../e2e/query_lifecycle_test.go)\n[Example](../examples/bubbletea/)\n```go\nsoroi.Get[User](client, key)\n```\n";
	for (const locale of ["zh-CN", "en"]) {
		const result = prepareMarkdown(source, chapters[1], locale, 2, "main");
		assert.match(result, /title: "Guide"/);
		assert.ok(!result.includes("# Guide"));
		assert.ok(!result.includes("[English]"));
		assert.ok(
			result.includes(`[Cache](/soroi/${locale === "en" ? "" : "zh-CN/"}docs/cache/#expiry)`),
		);
		assert.match(
			result,
			/https:\/\/github.com\/Waterkyuu\/soroi\/blob\/main\/e2e\/query_lifecycle_test.go/,
		);
		assert.ok(result.includes("soroi.Get[User](client, key)"));
		assert.ok(
			result.includes(
				"[Example](https://github.com/Waterkyuu/soroi/tree/main/examples/bubbletea/)",
			),
		);
	}
});

test("edit links point to maintained documentation rather than generated content", () => {
	assert.ok(
		prepareMarkdown("# Start", chapters[0], "en", 1, "main").includes(
			"docs/getting-started.en-US.md",
		),
	);
	assert.ok(
		prepareMarkdown("# Cache", chapters[2], "zh-CN", 3, "main").includes("docs/cache.zh-CN.md"),
	);
});

test("benchmark artifacts resolve to source files for both locales", () => {
	const chapter = chapters.find((chapter) => chapter.id === "business-benchmarks");
	assert.ok(chapter);
	for (const locale of ["en", "zh-CN"]) {
		const output = prepareMarkdown(
			"# Benchmark\n\n[Raw records](../benchmarks/business-darwin-arm64.jsonl)",
			chapter,
			locale,
			1,
			"main",
		);
		assert.ok(
			output.includes(
				"https://github.com/Waterkyuu/soroi/blob/main/benchmarks/business-darwin-arm64.jsonl",
			),
		);
	}
});
