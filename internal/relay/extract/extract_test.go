package extract

import "testing"

func TestOpenAIExtract(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		want   Usage
		wantOK bool
	}{
		{
			name: "chat completions",
			body: `{"model":"gpt-4o-mini","usage":{
				"prompt_tokens":20,"completion_tokens":5,"total_tokens":25,
				"prompt_tokens_details":{"cached_tokens":8}}}`,
			want:   Usage{Model: "gpt-4o-mini", InputTokens: 20, OutputTokens: 5, TotalTokens: 25, CachedInputTokens: 8},
			wantOK: true,
		},
		{
			// The Responses API renames every token field. Read as Chat
			// Completions this parses "successfully" into all-zero counts.
			name: "responses api non-streaming",
			body: `{"model":"gpt-5-mini","usage":{
				"input_tokens":20,"output_tokens":5,"total_tokens":25,
				"input_tokens_details":{"cached_tokens":8},
				"output_tokens_details":{"reasoning_tokens":3}}}`,
			want:   Usage{Model: "gpt-5-mini", InputTokens: 20, OutputTokens: 5, TotalTokens: 25, CachedInputTokens: 8},
			wantOK: true,
		},
		{
			// Terminal streaming event nests the payload under `response`.
			name: "responses api streaming completed event",
			body: `{"type":"response.completed","response":{"model":"gpt-5-mini","usage":{
				"input_tokens":11,"output_tokens":2,"total_tokens":13,
				"input_tokens_details":{"cached_tokens":0}}}}`,
			want:   Usage{Model: "gpt-5-mini", InputTokens: 11, OutputTokens: 2, TotalTokens: 13},
			wantOK: true,
		},
		{
			name:   "responses api in-progress event carries null usage",
			body:   `{"type":"response.in_progress","response":{"model":"gpt-5-mini","usage":null}}`,
			wantOK: false,
		},
		{
			name:   "streaming delta chunk has no usage",
			body:   `{"model":"gpt-4o-mini","choices":[{"delta":{"content":"hi"}}]}`,
			wantOK: false,
		},
		{
			// Must NOT report all-zero usage: that would price a real call at
			// $0 and record it as a legitimately free request.
			name:   "unrecognized usage shape is rejected, not zeroed",
			body:   `{"model":"some-model","usage":{"weird_tokens":42}}`,
			wantOK: false,
		},
		{
			name:   "malformed json",
			body:   `{"model":`,
			wantOK: false,
		},
		{
			// A genuine zero must still be reported, which is why token fields
			// are read through pointers rather than checked for non-zero.
			name:   "explicit zero output is real usage",
			body:   `{"model":"gpt-4o-mini","usage":{"prompt_tokens":7,"completion_tokens":0,"total_tokens":7}}`,
			want:   Usage{Model: "gpt-4o-mini", InputTokens: 7, OutputTokens: 0, TotalTokens: 7},
			wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := OpenAIExtract([]byte(tc.body))
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (got %+v)", ok, tc.wantOK, got)
			}
			if !tc.wantOK {
				return
			}
			if got.Model != tc.want.Model ||
				got.InputTokens != tc.want.InputTokens ||
				got.OutputTokens != tc.want.OutputTokens ||
				got.TotalTokens != tc.want.TotalTokens ||
				got.CachedInputTokens != tc.want.CachedInputTokens {
				t.Errorf("got model=%q in=%d out=%d total=%d cached=%d,\nwant model=%q in=%d out=%d total=%d cached=%d",
					got.Model, got.InputTokens, got.OutputTokens, got.TotalTokens, got.CachedInputTokens,
					tc.want.Model, tc.want.InputTokens, tc.want.OutputTokens, tc.want.TotalTokens, tc.want.CachedInputTokens)
			}
		})
	}
}

func TestGeminiExtract(t *testing.T) {
	body := `{"modelVersion":"gemini-2.5-flash","usageMetadata":{
		"promptTokenCount":6,"candidatesTokenCount":5,"thoughtsTokenCount":4,
		"totalTokenCount":15,"cachedContentTokenCount":2}}`

	got, ok := GeminiExtract([]byte(body))
	if !ok {
		t.Fatal("expected usage")
	}
	// Thinking tokens bill at the output rate, so they belong in OutputTokens.
	if got.OutputTokens != 9 {
		t.Errorf("OutputTokens = %d, want 9 (candidates 5 + thoughts 4)", got.OutputTokens)
	}
	if got.InputTokens != 6 || got.CachedInputTokens != 2 || got.TotalTokens != 15 {
		t.Errorf("got in=%d cached=%d total=%d, want 6/2/15", got.InputTokens, got.CachedInputTokens, got.TotalTokens)
	}
}

// Gemini's usage accounting is spread over five counts and every one is
// billable. These lock down the arithmetic — and that cached tokens are a
// SUBSET of the prompt, not an addend.
func TestGeminiExtractTokenAccounting(t *testing.T) {
	t.Run("thinking tokens are billed as output", func(t *testing.T) {
		// Shape observed live on gemini-2.5-flash: the visible answer is 17
		// tokens, the reasoning behind it 1121.
		body := []byte(`{"modelVersion":"gemini-2.5-flash","usageMetadata":{
			"promptTokenCount":13,"candidatesTokenCount":17,
			"thoughtsTokenCount":1121,"totalTokenCount":1151}}`)

		u, ok := GeminiExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.InputTokens != 13 {
			t.Errorf("input = %d, want 13", u.InputTokens)
		}
		if u.OutputTokens != 1138 {
			t.Errorf("output = %d, want 1138 (17 candidates + 1121 thoughts)", u.OutputTokens)
		}
		if got := u.InputTokens + u.OutputTokens; got != u.TotalTokens {
			t.Errorf("input+output = %d, want it to reconcile with totalTokenCount %d", got, u.TotalTokens)
		}
	})

	t.Run("cached tokens are a subset of the prompt", func(t *testing.T) {
		body := []byte(`{"modelVersion":"gemini-2.5-flash","usageMetadata":{
			"promptTokenCount":10000,"cachedContentTokenCount":9000,
			"candidatesTokenCount":50,"totalTokenCount":10050}}`)

		u, ok := GeminiExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		// Input must be the FULL prompt. Subtracting cache here would
		// double-discount, because Config.Cost already nets it out.
		if u.InputTokens != 10000 {
			t.Errorf("input = %d, want the full 10000 including cached", u.InputTokens)
		}
		if u.CachedInputTokens != 9000 {
			t.Errorf("cached = %d, want 9000", u.CachedInputTokens)
		}
		if u.CachedInputTokens > u.InputTokens {
			t.Error("cached exceeds input — they are not being treated as a subset")
		}
	})

	t.Run("tool-use prompt tokens are added to input", func(t *testing.T) {
		// toolUsePromptTokenCount is NOT part of promptTokenCount; dropping it
		// undercounts every grounded call.
		body := []byte(`{"modelVersion":"gemini-2.5-flash","usageMetadata":{
			"promptTokenCount":100,"toolUsePromptTokenCount":400,
			"candidatesTokenCount":20,"totalTokenCount":520}}`)

		u, ok := GeminiExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.InputTokens != 500 {
			t.Errorf("input = %d, want 500 (100 prompt + 400 tool-use)", u.InputTokens)
		}
		if got := u.InputTokens + u.OutputTokens; got != u.TotalTokens {
			t.Errorf("input+output = %d, want %d — tool-use tokens are unaccounted", got, u.TotalTokens)
		}
	})

	t.Run("usage carrying no counts is rejected, not zeroed", func(t *testing.T) {
		// Every count is omitempty, so a shape change yields an object with no
		// recognisable fields. Recording that as a $0 call would hide the break.
		for _, body := range []string{
			`{"modelVersion":"gemini-2.5-flash","usageMetadata":{"trafficType":"ON_DEMAND"}}`,
			`{"modelVersion":"gemini-2.5-flash","usageMetadata":{}}`,
		} {
			if u, ok := GeminiExtract([]byte(body)); ok {
				t.Errorf("accepted unrecognised usage %s as %+v; want rejection", body, u)
			}
		}
	})

	t.Run("explicit zero output is real usage", func(t *testing.T) {
		body := []byte(`{"modelVersion":"gemini-2.5-flash","usageMetadata":{
			"promptTokenCount":8,"candidatesTokenCount":0,"totalTokenCount":8}}`)

		u, ok := GeminiExtract(body)
		if !ok {
			t.Fatal("a real call that produced no output must still be metered")
		}
		if u.InputTokens != 8 || u.OutputTokens != 0 {
			t.Errorf("got input=%d output=%d, want 8/0", u.InputTokens, u.OutputTokens)
		}
	})
}

// Image generation via generateContent (gemini-3.1-flash-image, "Nano Banana
// 2") reports IMAGE-modality output tokens. These lock down that such a call is
// tagged image and priced on its image tokens — the case that otherwise books
// silently as a cheap text completion.
func TestGeminiExtractImage(t *testing.T) {
	t.Run("image call is tagged image and counts its output tokens", func(t *testing.T) {
		// Shape observed for an image generation: ~1290 output tokens = one image,
		// reported both as candidatesTokenCount and in the modality breakdown.
		body := []byte(`{"modelVersion":"gemini-3.1-flash-image","usageMetadata":{
			"promptTokenCount":8,"candidatesTokenCount":1290,"totalTokenCount":1298,
			"promptTokensDetails":[{"modality":"TEXT","tokenCount":8}],
			"candidatesTokensDetails":[{"modality":"IMAGE","tokenCount":1290}]}}`)

		u, ok := GeminiExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.Modality != ModalityImage {
			t.Errorf("modality = %q, want image", u.Modality)
		}
		if u.InputTokens != 8 || u.OutputTokens != 1290 || u.TotalTokens != 1298 {
			t.Errorf("got in=%d out=%d total=%d, want 8/1290/1298", u.InputTokens, u.OutputTokens, u.TotalTokens)
		}
	})

	t.Run("image tokens recovered when the flat count is absent", func(t *testing.T) {
		// Defensive: if a future response reports image tokens ONLY in the
		// modality breakdown, the call must still be billed for them, never as
		// a $0 output.
		body := []byte(`{"modelVersion":"gemini-3.1-flash-image","usageMetadata":{
			"promptTokenCount":8,"totalTokenCount":1298,
			"candidatesTokensDetails":[{"modality":"IMAGE","tokenCount":1290}]}}`)

		u, ok := GeminiExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.Modality != ModalityImage {
			t.Errorf("modality = %q, want image", u.Modality)
		}
		if u.OutputTokens != 1290 {
			t.Errorf("output = %d, want 1290 recovered from the modality breakdown", u.OutputTokens)
		}
	})

	t.Run("a text completion is left as llm", func(t *testing.T) {
		// TEXT-modality details must not flip a normal completion to image.
		body := []byte(`{"modelVersion":"gemini-2.5-flash","usageMetadata":{
			"promptTokenCount":6,"candidatesTokenCount":5,"totalTokenCount":11,
			"candidatesTokensDetails":[{"modality":"TEXT","tokenCount":5}]}}`)

		u, ok := GeminiExtract(body)
		if !ok {
			t.Fatal("expected usage")
		}
		if u.Modality != "" {
			t.Errorf("modality = %q, want empty (llm) for a text completion", u.Modality)
		}
		if u.OutputTokens != 5 {
			t.Errorf("output = %d, want 5", u.OutputTokens)
		}
	})
}
