package extract

import (
	"bytes"
	"testing"
)

func TestOpenAIImageExtract(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		want   Usage
		wantOK bool
	}{
		{
			// A gpt-image-1 generation. The response names no model, so Model is
			// left empty for the X-Usage-Model hint to fill. Output tokens are
			// image tokens, billed at $40/1M.
			name: "gpt-image-1 generation",
			body: `{"created":1713833628,"data":[{"b64_json":"aGVsbG8="}],
				"usage":{"total_tokens":4160,"input_tokens":25,"output_tokens":4135,
				"input_tokens_details":{"text_tokens":25,"image_tokens":0}}}`,
			want:   Usage{Modality: ModalityImage, InputTokens: 25, OutputTokens: 4135, TotalTokens: 4160},
			wantOK: true,
		},
		{
			// total_tokens absent: derive it so a row always reconciles.
			name:   "total derived when absent",
			body:   `{"data":[{"b64_json":"x"}],"usage":{"input_tokens":30,"output_tokens":1000}}`,
			want:   Usage{Modality: ModalityImage, InputTokens: 30, OutputTokens: 1000, TotalTokens: 1030},
			wantOK: true,
		},
		{
			name:   "no usage object is rejected",
			body:   `{"created":1,"data":[{"b64_json":"x"}]}`,
			wantOK: false,
		},
		{
			// A usage object matching neither count must not record a phantom
			// free generation.
			name:   "usage with no token counts is rejected, not zeroed",
			body:   `{"data":[],"usage":{"weird_tokens":9}}`,
			wantOK: false,
		},
		{
			name:   "malformed json",
			body:   `{"usage":`,
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := OpenAIImageExtract([]byte(tc.body))
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (got %+v)", ok, tc.wantOK, got)
			}
			if !tc.wantOK {
				return
			}
			if got.Modality != tc.want.Modality ||
				got.InputTokens != tc.want.InputTokens ||
				got.OutputTokens != tc.want.OutputTokens ||
				got.TotalTokens != tc.want.TotalTokens {
				t.Errorf("got modality=%q in=%d out=%d total=%d,\nwant modality=%q in=%d out=%d total=%d",
					got.Modality, got.InputTokens, got.OutputTokens, got.TotalTokens,
					tc.want.Modality, tc.want.InputTokens, tc.want.OutputTokens, tc.want.TotalTokens)
			}
		})
	}
}

// The response body is megabytes of inline base64; only the usage object may
// reach the ledger's metadata. Guard against Raw ever carrying the image bytes.
func TestOpenAIImageExtractRawExcludesBody(t *testing.T) {
	body := `{"created":1,"data":[{"b64_json":"QUJDREVGRw-pretend-this-is-4MB"}],
		"usage":{"input_tokens":25,"output_tokens":4135,"total_tokens":4160}}`

	got, ok := OpenAIImageExtract([]byte(body))
	if !ok {
		t.Fatal("expected usage")
	}
	if bytes.Contains(got.Raw, []byte("b64_json")) || bytes.Contains(got.Raw, []byte("pretend")) {
		t.Errorf("Raw carries the response body, not just the usage object: %s", got.Raw)
	}
	if !bytes.Contains(got.Raw, []byte("input_tokens")) {
		t.Errorf("Raw should hold the usage object, got: %s", got.Raw)
	}
}
